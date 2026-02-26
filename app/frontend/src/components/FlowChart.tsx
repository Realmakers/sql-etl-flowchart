import { useCallback, useMemo } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  BackgroundVariant,
} from 'reactflow';
import 'reactflow/dist/style.css';
import QueryNode from '@/components/nodes/QueryNode';
import JoinNode from '@/components/nodes/JoinNode';
import DownloadButton from '@/components/DownloadButton';

interface FlowChartProps {
  nodes: Node[];
  edges: Edge[];
}

export default function FlowChart({ nodes: initialNodes, edges: initialEdges }: FlowChartProps) {
  const nodeTypes = useMemo(() => ({
    queryNode: QueryNode,
    joinNode: JoinNode,
  }), []);

  const [nodes, , onNodesChange] = useNodesState(initialNodes);
  const [edges, , onEdgesChange] = useEdgesState(initialEdges);

  const onInit = useCallback((reactFlowInstance: { fitView: () => void }) => {
    setTimeout(() => {
      reactFlowInstance.fitView();
    }, 100);
  }, []);

  if (initialNodes.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full bg-gradient-to-br from-slate-50 to-blue-50 text-slate-400">
        <div className="text-6xl mb-4">🔍</div>
        <h3 className="text-xl font-bold text-slate-500 mb-2">等待解析SQL</h3>
        <p className="text-sm text-slate-400 text-center max-w-sm">
          在左侧输入SQL查询语句，点击"解析并生成流程图"按钮，即可生成ETL数据管道流程图
        </p>
        <div className="mt-6 flex flex-wrap items-center justify-center gap-4 text-xs text-slate-400">
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-slate-200 border border-slate-400" />
            <span>源表</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-purple-200 border border-purple-400" />
            <span>CTE</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-teal-200 border border-teal-400" />
            <span>子查询</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-pink-200 border border-pink-400" />
            <span>临时表</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-blue-200 border border-blue-400" />
            <span>主查询</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-emerald-200 border border-emerald-400" />
            <span>输出结果</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 bg-amber-400 border border-amber-500" style={{ transform: 'rotate(45deg)', borderRadius: 2 }} />
            <span>JOIN</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-blue-100 border border-blue-400" />
            <span>事实表</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-green-100 border border-green-400" />
            <span>维度表</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-6 border-t-2 border-dashed border-amber-400" />
            <span>UNION ALL</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onInit={onInit}
        nodeTypes={nodeTypes}
        fitView
        minZoom={0.1}
        maxZoom={2}
        defaultEdgeOptions={{
          animated: true,
        }}
        proOptions={{ hideAttribution: true }}
      >
        <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#CBD5E1" />
        <Controls className="!bg-white !shadow-lg !border !border-slate-200 !rounded-lg" />
        <MiniMap
          className="!bg-white !shadow-lg !border !border-slate-200 !rounded-lg"
          nodeColor={(node) => {
            if (node.type === 'joinNode') return '#F59E0B';
            const nodeType = (node.data as { nodeType?: string })?.nodeType;
            if (nodeType === 'source') return '#94A3B8';
            if (nodeType === 'cte') return '#A78BFA';
            if (nodeType === 'temp') return '#F472B6';
            if (nodeType === 'subquery') return '#2DD4BF';
            if (nodeType === 'main') return '#60A5FA';
            if (nodeType === 'output') return '#34D399';
            return '#CBD5E1';
          }}
          maskColor="rgba(0,0,0,0.08)"
        />
        <DownloadButton />
      </ReactFlow>
    </div>
  );
}