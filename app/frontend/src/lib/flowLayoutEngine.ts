import dagre from 'dagre';
import { type Node, type Edge, MarkerType, Position } from 'reactflow';
import { type ParsedSQL, type SubQuery, classifyTable } from './sqlParser';

export interface QueryNodeData {
  label: string;
  subQuery: SubQuery;
  nodeType: 'source' | 'cte' | 'subquery' | 'main' | 'output' | 'temp';
}

export interface JoinNodeData {
  label: string;
  joinType: string;
  condition: string;
}

const NODE_WIDTH = 420;
const NODE_MIN_HEIGHT = 200;
const JOIN_NODE_SIZE = 100;

function estimateNodeHeight(sq: SubQuery): number {
  const baseHeight = 80;
  const tableHeight = sq.tables.length * 36;
  const fieldHeight = Math.min(sq.fields.length, 8) * 24;
  const filterHeight = sq.filters.length * 28;
  const extraHeight = sq.groupBy.length > 0 ? 28 : 0;
  return Math.max(NODE_MIN_HEIGHT, baseHeight + tableHeight + fieldHeight + filterHeight + extraHeight + 40);
}

// 获取用于匹配的标准名称（忽略 ${} 和大小写）
function getMatchName(name: string): string {
  let cleaned = name.toLowerCase();
  // 去除 ${} 包装
  if (cleaned.startsWith('${') && cleaned.endsWith('}')) {
    cleaned = cleaned.substring(2, cleaned.length - 1);
  }
  return cleaned;
}

// Ensure a source node exists for a physical table, return its node id
function ensureSourceNode(
  tableName: string,
  alias: string,
  nodes: Node[],
  g: dagre.graphlib.Graph,
): string {
  // 使用原始表名作为 ID 基础，但去除点号
  const sourceId = `source_${tableName.toLowerCase().replace(/\./g, '_').replace(/[^a-z0-9_]/g, '')}`;
  if (!nodes.find(n => n.id === sourceId)) {
    const tableRef = {
      name: tableName,
      schema: tableName.includes('.') ? tableName.split('.')[0] : '',
      tableName: tableName.includes('.') ? tableName.split('.').slice(1).join('.') : tableName,
      alias: alias || tableName,
      type: classifyTable(tableName),
    };
    const sourceLabel = tableRef.schema || tableRef.name;
    nodes.push({
      id: sourceId,
      type: 'queryNode',
      data: {
        label: sourceLabel,
        subQuery: {
          id: sourceId, name: tableRef.name, isCTE: false, isSubQuery: false,
          tables: [tableRef], fields: [], joins: [], filters: [],
          groupBy: [], orderBy: [], dependsOn: [],
        },
        nodeType: 'source',
      } as QueryNodeData,
      position: { x: 0, y: 0 },
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
    });
    g.setNode(sourceId, { width: NODE_WIDTH * 0.7, height: 100 });
  }
  return sourceId;
}

export function buildFlowElements(parsed: ParsedSQL): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: 'TB', nodesep: 80, ranksep: 120, marginx: 40, marginy: 40 });

  const queryMap = new Map<string, SubQuery>();
  const nodeIdMap = new Map<string, string>(); // name/id -> node id

  // Add CTE nodes
  for (const cte of parsed.ctes) {
    queryMap.set(cte.name.toLowerCase(), cte);
    queryMap.set(cte.id, cte);
    nodeIdMap.set(cte.name.toLowerCase(), cte.id);
    nodeIdMap.set(cte.id, cte.id);

    const height = estimateNodeHeight(cte);
    nodes.push({
      id: cte.id,
      type: 'queryNode',
      data: { 
        label: cte.name, 
        subQuery: cte, 
        nodeType: cte.isTempTable ? 'temp' : 'cte' 
      } as QueryNodeData,
      position: { x: 0, y: 0 },
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
    });
    g.setNode(cte.id, { width: NODE_WIDTH, height });
  }

  // Add subquery nodes
  for (const sq of parsed.subQueries) {
    queryMap.set(sq.id, sq);
    nodeIdMap.set(sq.id, sq.id);

    const height = estimateNodeHeight(sq);
    nodes.push({
      id: sq.id,
      type: 'queryNode',
      data: { label: sq.name, subQuery: sq, nodeType: 'subquery' } as QueryNodeData,
      position: { x: 0, y: 0 },
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
    });
    g.setNode(sq.id, { width: NODE_WIDTH, height });
  }

  // Add main query node
  const mainId = parsed.mainQuery.id;
  const mainHeight = estimateNodeHeight(parsed.mainQuery);
  nodes.push({
    id: mainId,
    type: 'queryNode',
    data: { label: parsed.mainQuery.name, subQuery: parsed.mainQuery, nodeType: 'main' } as QueryNodeData,
    position: { x: 0, y: 0 },
    sourcePosition: Position.Bottom,
    targetPosition: Position.Top,
  });
  g.setNode(mainId, { width: NODE_WIDTH, height: mainHeight });

  // Add output node
  const outputId = 'output';
  nodes.push({
    id: outputId,
    type: 'queryNode',
    data: {
      label: '输出结果',
      subQuery: {
        id: outputId, name: '输出结果', isCTE: false, isSubQuery: false,
        tables: [], fields: parsed.mainQuery.fields, joins: [], filters: [],
        groupBy: [], orderBy: [], dependsOn: [],
      },
      nodeType: 'output',
    } as QueryNodeData,
    position: { x: 0, y: 0 },
    sourcePosition: Position.Bottom,
    targetPosition: Position.Top,
  });
  g.setNode(outputId, { width: NODE_WIDTH, height: 120 });

  // Build a set of UNION ALL source->target pairs for special edge styling
  // Also build a map of union sources (physical table names) per query
  const unionDeps = new Set<string>(); // "sourceNodeId->targetId"
  const unionPhysicalSources = new Map<string, string[]>(); // queryId -> physical table names in union

  for (const sq of parsed.allQueries) {
    if (sq.unionInfo) {
      const physicalSources: string[] = [];
      for (const src of sq.unionInfo.sources) {
        const srcMatch = getMatchName(src);
        
        // Find if this source matches any known node ID (CTE/Subquery)
        let foundNodeId = undefined;
        for (const [key, value] of nodeIdMap.entries()) {
             if (getMatchName(key) === srcMatch) {
                 foundNodeId = value;
                 break;
             }
        }

        if (foundNodeId) {
          // It's a known CTE/subquery
          unionDeps.add(`${foundNodeId}->${sq.id}`);
        } else {
          // It's a physical table
          physicalSources.push(src);
        }
      }
      if (physicalSources.length > 0) {
        unionPhysicalSources.set(sq.id, physicalSources);
      }
    }
  }

  // Track added edges to avoid duplicates
  const addedEdges = new Set<string>();
  let joinNodeCounter = 0;

  function addEdge(sourceId: string, targetId: string, options?: { isUnion?: boolean; unionLabel?: string }) {
    const edgeKey = `${sourceId}->${targetId}`;
    if (addedEdges.has(edgeKey)) return;
    addedEdges.add(edgeKey);

    if (options?.isUnion) {
      edges.push({
        id: `e_union_${sourceId}_${targetId}`,
        source: sourceId,
        target: targetId,
        label: options.unionLabel || 'UNION ALL',
        labelStyle: { fill: '#F59E0B', fontWeight: 700, fontSize: 11 },
        labelBgStyle: { fill: '#FFFBEB', stroke: '#F59E0B', strokeWidth: 1 },
        labelBgPadding: [6, 4] as [number, number],
        markerEnd: { type: MarkerType.ArrowClosed, color: '#F59E0B' },
        style: { stroke: '#F59E0B', strokeWidth: 2.5, strokeDasharray: '8 4' },
        animated: true,
      });
    } else {
      edges.push({
        id: `e_${sourceId}_${targetId}`,
        source: sourceId,
        target: targetId,
        markerEnd: { type: MarkerType.ArrowClosed, color: '#64748B' },
        style: { stroke: '#64748B', strokeWidth: 2 },
        animated: true,
      });
    }
    g.setEdge(sourceId, targetId);
  }

  // Process each query's dependencies
  for (const sq of parsed.allQueries) {
    const handledDeps = new Set<string>();

    // 确保临时表、CTE、子查询节点至少有一个输入边
    // 如果没有明确的 FROM/JOIN，我们仍然需要检查是否使用了任何物理表或上游节点
    
    // Collect the FROM table (first table, not from a JOIN) for JOIN diamond linking
    let fromTableNodeId: string | undefined;
    const joinTableNames = new Set(sq.joins.map(j => j.table.name.toLowerCase()));

    // Determine the FROM table (first table not in joins)
    for (const table of sq.tables) {
      const tableLower = getMatchName(table.name);
      const tableNameLower = getMatchName(table.tableName);

      // Check if tableLower is in nodeIdMap (fuzzy match)
      let knownNodeId = undefined;
      for (const [key, value] of nodeIdMap.entries()) {
          const keyMatch = getMatchName(key);
          if (keyMatch === tableLower || keyMatch === tableNameLower) {
              knownNodeId = value;
              break;
          }
      }

      // 如果是子查询引用（且该子查询已经作为节点存在），我们需要将其视为一种特殊的"源表"
      if (knownNodeId) {
          // It's a CTE or Subquery reference
          const isJoin = Array.from(joinTableNames).some(j => getMatchName(j) === tableLower);
          if (!isJoin) {
              fromTableNodeId = knownNodeId;
              break;
          }
      } else {
        // It's a physical table
        const isJoin = Array.from(joinTableNames).some(j => getMatchName(j) === tableLower);
        if (!isJoin) {
          // This is the FROM table
          fromTableNodeId = ensureSourceNode(table.name, table.alias, nodes, g);
          break;
        }
      }
    }

    // Handle JOIN-based dependencies: create diamond nodes
    // If there's a FROM table and JOINs, connect FROM table -> JOIN diamond -> target
    for (const join of sq.joins) {
      const tableLower = getMatchName(join.table.name);
      const tableNameLower = getMatchName(join.table.tableName);

      // Find the source node for this join table
      let joinSourceNodeId: string | undefined;
      
      // Fuzzy match in nodeIdMap
      for (const [key, value] of nodeIdMap.entries()) {
        const keyMatch = getMatchName(key);
        if (keyMatch === tableLower || keyMatch === tableNameLower) {
          joinSourceNodeId = value;
          break;
        }
      }

      if (!joinSourceNodeId) {
        // It's a physical source table
        joinSourceNodeId = ensureSourceNode(join.table.name, join.table.alias, nodes, g);
      }

      // Create JOIN diamond node
      const joinId = `join_${joinNodeCounter++}`;
      nodes.push({
        id: joinId,
        type: 'joinNode',
        data: { label: join.type, joinType: join.type, condition: join.condition } as JoinNodeData,
        position: { x: 0, y: 0 },
        sourcePosition: Position.Bottom,
        targetPosition: Position.Top,
      });
      g.setNode(joinId, { width: JOIN_NODE_SIZE, height: JOIN_NODE_SIZE });

      // Edge: FROM table -> join diamond (if we have a FROM table)
      if (fromTableNodeId) {
        const ek0 = `${fromTableNodeId}->${joinId}`;
        if (!addedEdges.has(ek0)) {
          addedEdges.add(ek0);
          edges.push({
            id: `e_${fromTableNodeId}_${joinId}`,
            source: fromTableNodeId,
            target: joinId,
            markerEnd: { type: MarkerType.ArrowClosed, color: '#64748B' },
            style: { stroke: '#64748B', strokeWidth: 2 },
            animated: true,
          });
          g.setEdge(fromTableNodeId, joinId);
        }
      }

      // Edge: join source table -> join diamond
      const ek1 = `${joinSourceNodeId}->${joinId}`;
      if (!addedEdges.has(ek1)) {
        addedEdges.add(ek1);
        edges.push({
          id: `e_${joinSourceNodeId}_${joinId}`,
          source: joinSourceNodeId,
          target: joinId,
          markerEnd: { type: MarkerType.ArrowClosed, color: '#64748B' },
          style: { stroke: '#64748B', strokeWidth: 2 },
          animated: true,
        });
        g.setEdge(joinSourceNodeId, joinId);
      }

      // Edge: join diamond -> current query
      const ek2 = `${joinId}->${sq.id}`;
      if (!addedEdges.has(ek2)) {
        addedEdges.add(ek2);
        edges.push({
          id: `e_${joinId}_${sq.id}`,
          source: joinId,
          target: sq.id,
          markerEnd: { type: MarkerType.ArrowClosed, color: '#64748B' },
          style: { stroke: '#64748B', strokeWidth: 2 },
          animated: true,
        });
        g.setEdge(joinId, sq.id);
      }

      // Mark dependencies as handled
      handledDeps.add(tableLower);
      handledDeps.add(tableNameLower);
      // Also mark the FROM table as handled (it's connected via the JOIN diamond)
      if (fromTableNodeId) {
        for (const table of sq.tables) {
          if (!joinTableNames.has(table.name.toLowerCase())) {
            handledDeps.add(table.name.toLowerCase());
            handledDeps.add(table.tableName.toLowerCase());
            break;
          }
        }
      }
    }

    // Handle UNION ALL physical table sources
    const physicalUnionSources = unionPhysicalSources.get(sq.id);
    if (physicalUnionSources) {
      for (const tableName of physicalUnionSources) {
        const sourceNodeId = ensureSourceNode(tableName, '', nodes, g);
        addEdge(sourceNodeId, sq.id, {
          isUnion: true,
          unionLabel: sq.unionInfo?.type || 'UNION ALL',
        });
        handledDeps.add(tableName.toLowerCase());
      }
    }

    // Handle dependsOn (CTE/subquery references not covered by JOINs or UNION)
    for (const dep of sq.dependsOn) {
      const depMatch = getMatchName(dep);
      if (Array.from(handledDeps).some(h => getMatchName(h) === depMatch)) continue;

      let depNodeId = undefined;
      for (const [key, value] of nodeIdMap.entries()) {
          if (getMatchName(key) === depMatch) {
              depNodeId = value;
              break;
          }
      }

      if (!depNodeId) continue;

      // 避免自引用
      if (depNodeId === sq.id) continue;
      
      // 避免重复连接（如果已经通过 JOIN 或 UNION 连接了）
      const ek = `${depNodeId}->${sq.id}`;
      if (addedEdges.has(ek)) continue;

      const isUnion = unionDeps.has(`${depNodeId}->${sq.id}`);
      addEdge(depNodeId, sq.id, {
        isUnion,
        unionLabel: sq.unionInfo?.type || 'UNION ALL',
      });
      handledDeps.add(dep);
    }

    // Handle remaining FROM tables that aren't CTE/subquery and not in JOINs
    for (const table of sq.tables) {
      const tableMatch = getMatchName(table.name);
      const tableNameMatch = getMatchName(table.tableName);

      if (Array.from(handledDeps).some(h => {
          const m = getMatchName(h);
          return m === tableMatch || m === tableNameMatch;
      })) continue;

      // Check if it's a known CTE/subquery
      let depNodeId = undefined;
      for (const [key, value] of nodeIdMap.entries()) {
          const m = getMatchName(key);
          if (m === tableMatch || m === tableNameMatch) {
              depNodeId = value;
              break;
          }
      }

      if (depNodeId) {
        if (depNodeId !== sq.id) {
          const ek = `${depNodeId}->${sq.id}`;
          if (!addedEdges.has(ek)) {
            addEdge(depNodeId, sq.id);
          }
        }
        handledDeps.add(table.name);
        continue;
      }

      // It's a physical source table (FROM without JOIN)
      const sourceId = ensureSourceNode(table.name, table.alias, nodes, g);
      addEdge(sourceId, sq.id);
      handledDeps.add(table.name);
    }
    
    // 检查是否有依赖但未连接的情况 (例如：CREATE TABLE AS SELECT ... FROM table)
    // 如果一个 CTE/Temp/Subquery 没有任何输入边，我们尝试连接其 tables 列表中的任何剩余表
    const incomingEdges = edges.filter(e => e.target === sq.id);
    if (incomingEdges.length === 0 && (sq.isCTE || sq.isTempTable || sq.isSubQuery)) {
       for (const table of sq.tables) {
         const tableMatch = getMatchName(table.name);
         const tableNameMatch = getMatchName(table.tableName);
         
         // 检查是否已经连接过（虽然上面逻辑应该覆盖了，但为了保险）
         let alreadyConnected = false;
         for (const edge of incomingEdges) {
             const sourceNode = nodes.find(n => n.id === edge.source);
             if (sourceNode) {
                 const labelMatch = getMatchName(sourceNode.data.label);
                 if (labelMatch === tableMatch || labelMatch === tableNameMatch) {
                     alreadyConnected = true;
                     break;
                 }
             }
         }
         if (alreadyConnected) continue;

         // 连接到上游节点（可能是 CTE 或 物理表）
         let sourceNodeId = undefined;
         for (const [key, value] of nodeIdMap.entries()) {
             const m = getMatchName(key);
             if (m === tableMatch || m === tableNameMatch) {
                 sourceNodeId = value;
                 break;
             }
         }

         if (!sourceNodeId) {
             sourceNodeId = ensureSourceNode(table.name, table.alias, nodes, g);
         }
         addEdge(sourceNodeId, sq.id);
       }
    }
  }

  // Connect main query to output
  addEdge(mainId, outputId);
  const mainOutputEdge = edges.find(e => e.source === mainId && e.target === outputId);
  if (mainOutputEdge) {
    mainOutputEdge.markerEnd = { type: MarkerType.ArrowClosed, color: '#3B82F6' };
    mainOutputEdge.style = { stroke: '#3B82F6', strokeWidth: 3 };
  }

  // Run dagre layout
  dagre.layout(g);

  // Apply positions
  for (const node of nodes) {
    const dagreNode = g.node(node.id);
    if (dagreNode) {
      node.position = {
        x: dagreNode.x - (dagreNode.width || 0) / 2,
        y: dagreNode.y - (dagreNode.height || 0) / 2,
      };
    }
  }

  return { nodes, edges };
}