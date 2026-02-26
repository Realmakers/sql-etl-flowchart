// SQL Parser - Parses SQL into structured ETL components

export interface TableRef {
  name: string;
  schema: string;
  tableName: string;
  alias: string;
  type?: 'fact' | 'dimension';
}

export interface FieldInfo {
  expression: string;
  alias: string;
  originalName: string;
  displayText: string;
  transformation: string;
}

export interface JoinInfo {
  type: string;
  table: TableRef;
  condition: string;
}

export interface FilterInfo {
  clause: string;
  condition: string;
}

export interface UnionInfo {
  type: 'UNION ALL' | 'UNION';
  sources: string[]; // CTE names, subquery ids, or physical table names
}

export interface SubQuery {
  id: string;
  name: string;
  isCTE: boolean;
  isSubQuery: boolean;
  isTempTable?: boolean; // 新增：标记是否为临时表
  tables: TableRef[];
  fields: FieldInfo[];
  joins: JoinInfo[];
  filters: FilterInfo[];
  groupBy: string[];
  orderBy: string[];
  dependsOn: string[];
  unionInfo?: UnionInfo;
}

export interface ParsedSQL {
  ctes: SubQuery[];
  mainQuery: SubQuery;
  subQueries: SubQuery[];
  allQueries: SubQuery[];
}

export function classifyTable(fullName: string): 'fact' | 'dimension' {
  const lowerName = fullName.toLowerCase();
  // 规则(1)：如果表名包含 'app','dm','dwd','dws' 中的任意一个，则识别为事实表
  if (lowerName.includes('app') || lowerName.includes('dm') || lowerName.includes('dwd') || lowerName.includes('dws')) {
    return 'fact';
  }
  // 其他情况（包括包含 'dim' 或其他任意字符），将被识别为 维度表
  return 'dimension';
}

function parseTableName(raw: string): { fullName: string; schema: string; tableName: string } {
  const dotIndex = raw.indexOf('.');
  if (dotIndex !== -1) {
    return { fullName: raw, schema: raw.substring(0, dotIndex), tableName: raw.substring(dotIndex + 1) };
  }
  return { fullName: raw, schema: '', tableName: raw };
}

function createTableRef(rawName: string, alias: string, knownNames?: string[]): TableRef {
  // 预处理：清洗 rawName，去除引号
  const cleanedRawName = cleanName(rawName);
  const { fullName, schema, tableName } = parseTableName(cleanedRawName);
  // Check if alias is a keyword or invalid, if so, ignore it
  const invalidAliases = ['WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'UNION', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'CROSS', 'JOIN', 'ON', 'LATERAL', 'VIEW', 'EXPLODE', 'UNNEST'];
  const finalAlias = (alias && !invalidAliases.includes(alias.toUpperCase())) ? alias : fullName;
  
  // 规则(2) & (3)：如果是 CTE/子查询引用（在 knownNames 中），则不设置 type (undefined)
  // 如果是物理表（不在 knownNames 中），则进行分类
  let type: 'fact' | 'dimension' | undefined;
  
  if (knownNames) {
      const lookupName = getMatchName(tableName);
      const lookupFull = getMatchName(fullName);
      
      const isKnown = knownNames.some(k => {
          const matchK = getMatchName(k);
          return matchK === lookupName || matchK === lookupFull;
      });
      
      if (!isKnown) {
        type = classifyTable(fullName);
      }
      // 如果是 known CTE/Subquery，保持 type 为 undefined
      // 但我们需要确保 Subquery/CTE 也作为表被引用，所以不再跳过它们，而是将它们视为"普通表"（无type）
      // 这已经在当前逻辑中实现：type 默认为 undefined，QueryNode 会使用 default 样式
    } else {
      // 兼容旧调用方式或无上下文情况，默认当作物理表处理
      type = classifyTable(fullName);
    }

  return { name: fullName, schema, tableName, alias: finalAlias, type };
}

function removeComments(sql: string): string {
  return sql.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
}

function normalizeWhitespace(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim();
}

function findMatchingParen(sql: string, start: number): number {
  let depth = 0;
  for (let i = start; i < sql.length; i++) {
    if (sql[i] === '(') depth++;
    if (sql[i] === ')') { depth--; if (depth === 0) return i; }
  }
  return -1;
}

function extractCTEs(sql: string): { ctes: { name: string; body: string }[]; remainingSQL: string } {
  const ctes: { name: string; body: string }[] = [];
  const withMatch = sql.match(/^\s*WITH\s+/i);
  if (!withMatch) return { ctes, remainingSQL: sql };

  let pos = withMatch[0].length;
  let remaining = sql;

  while (pos < sql.length) {
    const nameMatch = sql.substring(pos).match(/^\s*(\w+)\s+AS\s*\(/i);
    if (!nameMatch) break;
    const cteName = cleanName(nameMatch[1]);
    pos += nameMatch[0].length - 1;
    const closePos = findMatchingParen(sql, pos);
    if (closePos === -1) break;
    ctes.push({ name: cteName, body: sql.substring(pos + 1, closePos).trim() });
    pos = closePos + 1;
    const afterCTE = sql.substring(pos).match(/^\s*,\s*/);
    if (afterCTE) { pos += afterCTE[0].length; } else { remaining = sql.substring(pos).trim(); break; }
  }
  return { ctes, remainingSQL: remaining };
}

const TABLE_NAME_PATTERN = '(\\w+(?:\\.\\w+)*)';

// Split SQL by UNION (ALL) at the top level
function splitByUnion(sql: string): { parts: string[]; unionType: 'UNION ALL' | 'UNION' | null } {
  const parts: string[] = [];
  let depth = 0;
  let current = '';
  let i = 0;
  let detectedType: 'UNION ALL' | 'UNION' | null = null;

  while (i < sql.length) {
    if (sql[i] === '(') depth++;
    if (sql[i] === ')') depth--;
    if (depth === 0) {
      const remaining = sql.substring(i);
      const uaMatch = remaining.match(/^UNION\s+ALL\s/i);
      const uMatch = remaining.match(/^UNION\s(?!ALL)/i);
      if (uaMatch) { parts.push(current.trim()); current = ''; i += uaMatch[0].length; detectedType = 'UNION ALL'; continue; }
      if (uMatch) { parts.push(current.trim()); current = ''; i += uMatch[0].length; detectedType = 'UNION'; continue; }
    }
    current += sql[i];
    i++;
  }
  if (current.trim()) parts.push(current.trim());
  return { parts, unionType: parts.length > 1 ? detectedType : null };
}

// 清洗表名，处理引号和变量引用 ${...}
function cleanName(name: string): string {
  if (!name) return name;
  let trimmed = name.trim();
  
  // 处理引号
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed[trimmed.length - 1];
    if ((first === '"' && last === '"') || 
        (first === "'" && last === "'") || 
        (first === '`' && last === '`')) {
      trimmed = trimmed.substring(1, trimmed.length - 1);
    }
  }

  // 处理 ${...} 变量引用
  // 如果是 ${var} 格式，我们提取 var 作为名字，以便匹配
  // 或者保留原样，但在比较时需要注意。这里我们选择标准化：保留原样，但提供一个辅助函数来获取"核心名"用于匹配
  // 考虑到用户需求是让 ${base_table} 和 base_table 能匹配上，或者至少让 ${base_table} 被正确识别为表名
  // 这里我们不去除 ${}，因为这可能导致和非变量的同名表冲突。
  // 用户的核心痛点是 ${base_table} 没有被正确识别为表名，或者 create table ${base_table} 和 select ... from ${base_table} 没匹配上
  
  return trimmed;
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

function splitStatements(sql: string): string[] {
  const statements: string[] = [];
  let depth = 0;
  let current = '';
  let inQuote = false;
  let quoteChar = '';
  
  for (let i = 0; i < sql.length; i++) {
    const char = sql[i];
    
    if (inQuote) {
      if (char === quoteChar) {
        // 简单的转义处理：如果前一个字符不是反斜杠
        if (i > 0 && sql[i-1] !== '\\') {
          inQuote = false;
        }
      }
      current += char;
      continue;
    }
    
    if (char === "'" || char === '"' || char === '`') {
      inQuote = true;
      quoteChar = char;
      current += char;
      continue;
    }
    
    if (char === '(') depth++;
    if (char === ')') depth--;
    
    if (char === ';' && depth === 0) {
      if (current.trim()) statements.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  if (current.trim()) statements.push(current.trim());
  
  return statements;
}

function preprocessTempTables(sql: string): { tempTables: { name: string; body: string }[]; remainingSQL: string } {
  const tempTables: { name: string; body: string }[] = [];
  const statements = splitStatements(sql);
  const nonTempStatements: string[] = [];
  
  for (const stmt of statements) {
      // 匹配 CREATE TABLE [IF NOT EXISTS] table_name AS ...
    // 注意：match只做简单匹配，具体body提取靠后面的逻辑
    const createTableRegex = /^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)(?:\s+AS)?\s+((?:SELECT|WITH)[\s\S]+)$/i;
    const match = stmt.match(createTableRegex);
    
    if (match) {
      // 提取表名
      const rawTableName = match[1];
      const tableName = cleanName(rawTableName);
      const body = match[2];
      
      if (body) {
          // 去除可能包裹的括号
          let cleanedBody = body.trim();
          if (cleanedBody.startsWith('(') && cleanedBody.endsWith(')') && findMatchingParen(cleanedBody, 0) === cleanedBody.length - 1) {
              cleanedBody = cleanedBody.substring(1, cleanedBody.length - 1);
          }
          
          tempTables.push({ name: tableName, body: cleanedBody });
      }
    } else {
      if (stmt.trim()) {
        nonTempStatements.push(stmt);
      }
    }
  }
  
  return { tempTables, remainingSQL: nonTempStatements.join(';\n') };
}

// Detect UNION ALL at top level - captures ALL table names from each branch (not just known CTEs)
function detectUnionAllTopLevel(sql: string, knownNames: string[]): UnionInfo | undefined {
  const { parts, unionType } = splitByUnion(sql);
  if (parts.length <= 1 || !unionType) return undefined;

  const sources: string[] = [];

  for (const part of parts) {
    const trimmed = part.trim();
    // Extract FROM table reference from each UNION branch
    const fromMatch = trimmed.match(/FROM\s+(\w+(?:\.\w+)*)/i);
    if (fromMatch) {
      const tableName = fromMatch[1];
      // Use the lowercase CTE name if it's a known CTE, otherwise use original name
      const lower = getMatchName(tableName);
      
      const knownMatch = knownNames.find(k => getMatchName(k) === lower);
      
      if (knownMatch) {
        sources.push(knownMatch);
      } else {
        // Physical table - use original name (preserve case for display)
        sources.push(tableName);
      }
    }
  }

  if (sources.length > 0) {
    return { type: unionType, sources };
  }
  return undefined;
}

// Detect UNION ALL inside FROM subquery
function detectUnionAllInSubquery(sql: string, knownNames: string[]): UnionInfo | undefined {
  const regex = /FROM\s*\(/gi;
  let m;
  while ((m = regex.exec(sql)) !== null) {
    const open = m.index + m[0].length - 1;
    const close = findMatchingParen(sql, open);
    if (close === -1) continue;
    const body = sql.substring(open + 1, close).trim();
    const info = detectUnionAllTopLevel(body, knownNames);
    if (info) return info;
  }
  return undefined;
}

function detectUnionAll(sql: string, knownNames: string[]): UnionInfo | undefined {
  const n = normalizeWhitespace(sql);
  return detectUnionAllTopLevel(n, knownNames) || detectUnionAllInSubquery(n, knownNames);
}

// Replace parenthesized subqueries with placeholder to avoid parsing their internals
function replaceSubqueries(sql: string): { replaced: string; subs: Map<string, { body: string; alias: string; context: 'from' | 'join'; joinType?: string; joinCondition?: string }> } {
  const subs = new Map<string, { body: string; alias: string; context: 'from' | 'join'; joinType?: string; joinCondition?: string }>();
  let result = sql;
  let counter = 0;

  // Process FROM ( SELECT ... ) alias
  let changed = true;
  while (changed) {
    changed = false;
    const fromSubRegex = /FROM\s*\(/gi;
    let m;
    while ((m = fromSubRegex.exec(result)) !== null) {
      const open = m.index + m[0].length - 1;
      const close = findMatchingParen(result, open);
      if (close === -1) continue;
      const body = result.substring(open + 1, close).trim();
      if (!/^\s*SELECT\s/i.test(body)) continue;

      const afterParen = result.substring(close + 1);
      const aliasMatch = afterParen.match(/^\s*(?:AS\s+)?(\w+)/i);
      const alias = aliasMatch ? aliasMatch[1] : '';

      const placeholder = `__SUBQ_${counter++}__`;
      subs.set(placeholder, { body, alias, context: 'from' });

      const beforeFrom = result.substring(0, m.index);
      const afterAlias = aliasMatch ? result.substring(close + 1 + aliasMatch[0].length) : result.substring(close + 1);
      result = `${beforeFrom}FROM ${placeholder} ${alias}${afterAlias}`;
      changed = true;
      break;
    }
  }

  // Process JOIN ( SELECT ... ) alias ON ...
  changed = true;
  while (changed) {
    changed = false;
    const joinSubRegex = /((?:INNER|LEFT|RIGHT|FULL|CROSS)\s+)?JOIN\s*\(/gi;
    let m;
    while ((m = joinSubRegex.exec(result)) !== null) {
      const joinType = (m[1] || 'INNER ').trim() + ' JOIN';
      const open = m.index + m[0].length - 1;
      const close = findMatchingParen(result, open);
      if (close === -1) continue;
      const body = result.substring(open + 1, close).trim();
      if (!/^\s*SELECT\s/i.test(body)) continue;

      const afterParen = result.substring(close + 1);
      const aliasOnMatch = afterParen.match(/^\s*(?:AS\s+)?(\w+)\s+ON\s+([\s\S]*?)(?=(?:(?:INNER|LEFT|RIGHT|FULL|CROSS)\s+)?JOIN\s|WHERE\s|GROUP\s|HAVING\s|ORDER\s|LIMIT\s|UNION\s|$)/i);
      const alias = aliasOnMatch ? aliasOnMatch[1] : '';
      const joinCondition = aliasOnMatch ? aliasOnMatch[2].trim() : '';

      const placeholder = `__SUBQ_${counter++}__`;
      subs.set(placeholder, { body, alias, context: 'join', joinType, joinCondition });

      const beforeJoin = result.substring(0, m.index);
      const joinKeyword = m[0].replace(/\s*\($/, '');
      if (aliasOnMatch) {
        const afterOnCond = result.substring(close + 1 + aliasOnMatch[0].length);
        result = `${beforeJoin}${joinKeyword} ${placeholder} ${alias} ON ${joinCondition}${afterOnCond}`;
      } else {
        const afterClose = result.substring(close + 1);
        result = `${beforeJoin}${joinKeyword} ${placeholder}${afterClose}`;
      }
      changed = true;
      break;
    }
  }

  return { replaced: result, subs };
}

// Parse a flat SELECT (no nested subqueries - they've been replaced with placeholders)
function parseFlatSelect(
  sql: string,
  id: string,
  name: string,
  isCTE: boolean,
  isSubQuery: boolean,
  knownNames: string[],
): SubQuery {
  const normalized = normalizeWhitespace(sql);
  const tables: TableRef[] = [];
  const fields: FieldInfo[] = [];
  const joins: JoinInfo[] = [];
  const filters: FilterInfo[] = [];
  const groupBy: string[] = [];
  const orderBy: string[] = [];
  const dependsOn: string[] = [];

  // Detect UNION ALL (now captures physical table names too)
  const unionInfo = detectUnionAll(normalized, knownNames);
  if (unionInfo) {
    // Check if it is a complex UNION ALL (where sources are subqueries/complex SELECTs, not just table references)
    // If the split parts contain more than just a simple SELECT * FROM ..., we should treat them as subqueries.
    const { parts: unionParts } = splitByUnion(normalized);
    let isComplex = false;
    for (const part of unionParts) {
       // Heuristic: if part contains GROUP BY, JOIN, WHERE, HAVING, or nested SELECT, it's complex
       if (/GROUP\s+BY|JOIN|WHERE|HAVING|\(SELECT/i.test(part)) {
           isComplex = true;
           break;
       }
    }

    if (isComplex) {
       // If complex, we don't treat sources as direct dependencies yet.
       // Instead, we will parse each part as a subquery below.
    } else {
        for (const src of unionInfo.sources) {
          const srcMatch = getMatchName(src);
          
          const knownMatch = knownNames.find(k => getMatchName(k) === srcMatch);
          
          if (knownMatch) {
            if (!dependsOn.includes(knownMatch)) dependsOn.push(knownMatch);
          }
          // Physical tables in union sources will be handled by the flow layout engine
        }
    }
  }

  // For UNION ALL queries, parse each branch
  const { parts: unionParts } = splitByUnion(normalized);
  if (unionParts.length > 1) {
    // Check complexity again
    let isComplex = false;
    for (const part of unionParts) {
       if (/GROUP\s+BY|JOIN|WHERE|HAVING|\(SELECT/i.test(part)) {
           isComplex = true;
           break;
       }
    }

    if (isComplex) {
        // Treat each branch as a subquery
        // We need to recursively process each part and add it as a dependency
        // However, `processSQL` expects a full query string and adds to `subQueries` list.
        // We are currently inside `parseFlatSelect` which returns a single SubQuery object.
        // This suggests `parseFlatSelect` might be too low-level for this.
        // But we can simulate it: create subqueries for each branch, add them to `dependsOn`, 
        // and add them to `unionInfo.sources`.

        const newSources: string[] = [];
        
        // We need access to the global `subQueries` list to push new subqueries.
        // But `parseFlatSelect` doesn't have access to `subQueryCollector` or `subQueryCounter`.
        // We need to refactor `processSQL` to handle this, OR we can hack it here if we passed them down.
        // Currently `parseFlatSelect` signature: (sql, id, name, isCTE, isSubQuery, knownNames)
        // It does NOT have the collector.
        
        // REFACTOR STRATEGY: 
        // We cannot easily create new full subqueries here without the collector.
        // However, `processSQL` calls `parseFlatSelect`.
        // Maybe we should handle the complex UNION splitting inside `processSQL` BEFORE calling `parseFlatSelect`.
    } else {
        // Simple UNION: extract tables from each branch (existing logic)
        for (const part of unionParts) {
          const fromMatch = part.match(new RegExp(`FROM\\s+${TABLE_NAME_PATTERN}(\\s+(?:AS\\s+)?(\\w+))?`, 'i'));
          if (fromMatch) {
            // ... existing extraction logic ...
            const rawTableName = fromMatch[1];
            // Skip placeholders
            if (rawTableName.startsWith('__SUBQ_')) continue;
            let alias = fromMatch[3] || '';
            if (/^(WHERE|GROUP|HAVING|ORDER|LIMIT|UNION|LEFT|RIGHT|INNER|FULL|CROSS|JOIN)$/i.test(alias)) {
                alias = '';
            }
            const tableRef = createTableRef(rawTableName, alias, knownNames);
            // Avoid duplicates
            if (!tables.find(t => t.name === tableRef.name)) {
                tables.push(tableRef);
            }

            const lookupName = getMatchName(tableRef.tableName);
            const lookupFull = getMatchName(tableRef.name);
            
            const knownMatch = knownNames.find(n => {
                const m = getMatchName(n);
                return m === lookupName || m === lookupFull;
            });
            
            if (knownMatch) {
                const depName = knownMatch;
                if (!dependsOn.includes(depName)) dependsOn.push(depName);
            }
          }
        }
    }
  }

  // Extract SELECT fields
  const selectMatch = normalized.match(/SELECT\s+(DISTINCT\s+)?([\s\S]*?)\s+FROM\s/i);
  if (selectMatch) {
    const parsedFields = splitTopLevel(selectMatch[2], ',');
    for (const f of parsedFields) {
      const trimmed = f.trim();
      if (trimmed) fields.push(parseField(trimmed));
    }
  }

  // Extract FROM table (for non-UNION queries, or as the primary table)
  if (unionParts.length <= 1) {
    const fromRegex = new RegExp(`FROM\\s+${TABLE_NAME_PATTERN}(\\s+(?:AS\\s+)?(\\w+))?`, 'i');
    const fromMatch = normalized.match(fromRegex);
    if (fromMatch) {
      const rawTableName = fromMatch[1];
      // Skip placeholders
      if (!rawTableName.startsWith('__SUBQ_')) {
        let alias = fromMatch[3] || '';
        // Check if alias is a keyword, if so, ignore it
        if (/^(WHERE|GROUP|HAVING|ORDER|LIMIT|UNION|LEFT|RIGHT|INNER|FULL|CROSS|JOIN)$/i.test(alias)) {
            alias = '';
        }

        const tableRef = createTableRef(rawTableName, alias || rawTableName, knownNames);
        if (!tables.find(t => t.name === tableRef.name)) {
          tables.push(tableRef);
        }

      const lookupName = getMatchName(tableRef.tableName);
      const lookupFull = getMatchName(tableRef.name);
      
      const knownMatch = knownNames.find(n => {
          const m = getMatchName(n);
          return m === lookupName || m === lookupFull;
      });
      
      if (knownMatch) {
        const depName = knownMatch;
        if (!dependsOn.includes(depName)) dependsOn.push(depName);
      }
      }
    }
  }

  // Extract JOINs
  const joinRegex = new RegExp(
    `((?:INNER|LEFT|RIGHT|FULL|CROSS)\\s+)?JOIN\\s+${TABLE_NAME_PATTERN}(\\s+(?:AS\\s+)?(\\w+))?\\s+ON\\s+([\\s\\S]*?)(?=(?:(?:INNER|LEFT|RIGHT|FULL|CROSS)\\s+)?JOIN\\s|WHERE\\s|GROUP\\s|HAVING\\s|ORDER\\s|LIMIT\\s|UNION\\s|$)`,
    'gi'
  );
  let joinMatch;
  while ((joinMatch = joinRegex.exec(normalized)) !== null) {
    const joinType = (joinMatch[1] || 'INNER ').trim() + ' JOIN';
    const rawTableName = joinMatch[2];
    // Skip placeholders
    if (rawTableName.startsWith('__SUBQ_')) continue;
    let alias = joinMatch[4] || '';
    if (/^(WHERE|GROUP|HAVING|ORDER|LIMIT|UNION|LEFT|RIGHT|INNER|FULL|CROSS|JOIN)$/i.test(alias)) {
        alias = '';
    }
    const condition = joinMatch[5].trim().replace(/\s+$/, '');
    const tableRef = createTableRef(rawTableName, alias || rawTableName, knownNames);
    if (!tables.find(t => t.name === tableRef.name)) {
      tables.push(tableRef);
    }
    joins.push({ type: joinType, table: tableRef, condition });

    const lookupName = getMatchName(tableRef.tableName);
    const lookupFull = getMatchName(tableRef.name);
    
    const knownMatch = knownNames.find(n => {
        const m = getMatchName(n);
        return m === lookupName || m === lookupFull;
    });
    
    if (knownMatch) {
      const depName = knownMatch;
      if (!dependsOn.includes(depName)) dependsOn.push(depName);
    }
  }

  // Extract WHERE
  const whereMatch = normalized.match(/WHERE\s+([\s\S]*?)(?=GROUP\s|HAVING\s|ORDER\s|LIMIT\s|UNION\s|$)/i);
  if (whereMatch) filters.push({ clause: 'WHERE', condition: whereMatch[1].trim() });

  // Extract HAVING
  const havingMatch = normalized.match(/HAVING\s+([\s\S]*?)(?=ORDER\s|LIMIT\s|UNION\s|$)/i);
  if (havingMatch) filters.push({ clause: 'HAVING', condition: havingMatch[1].trim() });

  // Extract GROUP BY
  const groupByMatch = normalized.match(/GROUP\s+BY\s+([\s\S]*?)(?=HAVING\s|ORDER\s|LIMIT\s|UNION\s|$)/i);
  if (groupByMatch) groupBy.push(...groupByMatch[1].split(',').map(s => s.trim()).filter(Boolean));

  // Extract ORDER BY
  const orderByMatch = normalized.match(/ORDER\s+BY\s+([\s\S]*?)(?=LIMIT\s|UNION\s|$)/i);
  if (orderByMatch) orderBy.push(...orderByMatch[1].split(',').map(s => s.trim()).filter(Boolean));

  return {
    id, name, isCTE, isSubQuery,
    tables, fields, joins, filters, groupBy, orderBy,
    dependsOn: [...new Set(dependsOn)],
    unionInfo,
  };
}

// Recursively process a SQL body: extract subqueries, then parse the flat remainder
function processSQL(
  sql: string,
  id: string,
  name: string,
  isCTE: boolean,
  isSubQuery: boolean,
  knownNames: string[],
  subQueryCollector: SubQuery[],
  subQueryCounter: { count: number },
  processedBodies: Map<string, string>,
): SubQuery {
  const normalized = normalizeWhitespace(sql);

  // Replace nested subqueries with placeholders
  const { replaced, subs } = replaceSubqueries(normalized);

  // Process each extracted subquery recursively
  for (const [placeholder, info] of subs.entries()) {
    const bodyKey = normalizeWhitespace(info.body);
    const existingId = processedBodies.get(bodyKey);

    let subId: string;
    if (existingId) {
      subId = existingId;
    } else {
      subId = `subquery_${subQueryCounter.count++}`;
      const subName = info.alias ? `子查询 (${info.alias})` : `子查询_${subQueryCounter.count}`;

      const subQuery = processSQL(
        info.body, subId, subName, false, true,
        [...knownNames],
        subQueryCollector, subQueryCounter, processedBodies,
      );
      subQueryCollector.push(subQuery);
      processedBodies.set(bodyKey, subId);
    }

    if (!knownNames.includes(subId)) {
      knownNames.push(subId);
    }
  }

  // Parse the flat SQL (with placeholders replaced)
  // Check for complex UNION ALL structure FIRST
  const { parts: unionParts, unionType } = splitByUnion(normalized);
  
  // Heuristic: if any part contains GROUP BY, JOIN, WHERE, HAVING, or nested SELECT, it's complex
  let isComplexUnion = false;
  if (unionParts.length > 1) {
    for (const part of unionParts) {
       if (/GROUP\s+BY|JOIN|WHERE|HAVING|\(SELECT/i.test(part)) {
           isComplexUnion = true;
           break;
       }
    }
  }

  if (isComplexUnion && unionType) {
      // It's a complex UNION query.
      // We create a "main" node for this UNION, and then create subqueries for each branch.
      // 1. Create subqueries for each branch
      const unionSourceIds: string[] = [];
      
      for (let i = 0; i < unionParts.length; i++) {
          const part = unionParts[i];
          const branchId = `${id}_union_${i}`;
          const branchName = `${name} (分支 ${i+1})`;
          
          // Recursively process the branch
          // Note: we don't add to subQueryCollector here immediately if we want them to be top-level?
          // Actually, we should add them to subQueryCollector so they appear in the graph.
          
          // But wait, processSQL is recursive.
          // We can call processSQL for each branch.
          const branchQuery = processSQL(
              part, branchId, branchName, false, true,
              [...knownNames], subQueryCollector, subQueryCounter, processedBodies
          );
          
          // Add to collector
          subQueryCollector.push(branchQuery);
          unionSourceIds.push(branchId);
      }

      return {
          id, name, isCTE, isSubQuery,
          tables: [],
          fields: [], // Fields implied from branches
          joins: [],
          filters: [],
          groupBy: [],
          orderBy: [],
          dependsOn: unionSourceIds,
          unionInfo: {
              type: unionType,
              sources: unionSourceIds
          }
      };
  }

  const flatQuery = parseFlatSelect(replaced, id, name, isCTE, isSubQuery, knownNames);

  // Fix up: replace placeholder table refs with actual subquery dependencies
  for (const [placeholder, info] of subs.entries()) {
    const bodyKey = normalizeWhitespace(info.body);
    const subId = processedBodies.get(bodyKey)!;

    // Remove placeholder from tables
    const placeholderIdx = flatQuery.tables.findIndex(t => t.name === placeholder || t.tableName === placeholder);
    if (placeholderIdx !== -1) {
      flatQuery.tables.splice(placeholderIdx, 1);
    }

    // Remove placeholder from joins
    const joinIdx = flatQuery.joins.findIndex(j => j.table.name === placeholder || j.table.tableName === placeholder);
    if (joinIdx !== -1) {
      flatQuery.joins.splice(joinIdx, 1);
    }

    // Remove placeholder from dependsOn
    flatQuery.dependsOn = flatQuery.dependsOn.filter(d => d !== placeholder.toLowerCase());

    // Add actual subquery dependency
    if (!flatQuery.dependsOn.includes(subId)) {
      flatQuery.dependsOn.push(subId);
    }

    // If the subquery was in a JOIN context, add a join entry for it
    if (info.context === 'join' && info.joinType && info.joinCondition) {
      // 关键修改：即使是子查询，也作为表添加到 tables 列表中
      // 我们需要确保 subId (子查询名) 被加入 tables，这样 QueryNode 才能渲染它
      const subQueryRef = createTableRef(subId, info.alias || subId, knownNames);
      
      // 避免重复添加
      if (!flatQuery.tables.find(t => t.name === subQueryRef.name)) {
        flatQuery.tables.push(subQueryRef);
      }

      flatQuery.joins.push({
        type: info.joinType,
        table: subQueryRef,
        condition: info.joinCondition,
      });
    } else if (info.context === 'from') {
       // 关键修改：对于 FROM 子句中的子查询，也需要添加到 tables 列表
       const subQueryRef = createTableRef(subId, info.alias || subId, knownNames);
       if (!flatQuery.tables.find(t => t.name === subQueryRef.name)) {
         flatQuery.tables.push(subQueryRef);
       }
    }
  }

  return flatQuery;
}

function parseField(fieldStr: string): FieldInfo {
  const trimmed = fieldStr.trim();

  const asMatch = trimmed.match(/^([\s\S]+?)\s+AS\s+(\w+)$/i);
  if (asMatch) {
    const expression = asMatch[1].trim();
    const alias = asMatch[2];
    return { expression, alias, originalName: expression, displayText: `${expression} AS ${alias}`, transformation: detectTransformation(expression) };
  }

  const implicitMatch = trimmed.match(/^([\s\S]+?)\s+(\w+)$/i);
  if (implicitMatch) {
    const potentialExpr = implicitMatch[1].trim();
    const potentialAlias = implicitMatch[2];
    if (potentialExpr.endsWith(')') || /[+\-*/|]/.test(potentialExpr) || /\b(CASE|WHEN|THEN|END|OVER|SUM|COUNT|AVG|MAX|MIN|CONCAT|COALESCE|NVL|ROW_NUMBER|RANK|COLLECT_LIST|CONCAT_WS|CAST)\b/i.test(potentialExpr)) {
      return { expression: potentialExpr, alias: potentialAlias, originalName: potentialExpr, displayText: `${potentialExpr} AS ${potentialAlias}`, transformation: detectTransformation(potentialExpr) };
    }
    if (/^\w+\.\w+$/.test(potentialExpr)) {
      return { expression: potentialExpr, alias: potentialAlias, originalName: potentialExpr, displayText: `${potentialExpr} AS ${potentialAlias}`, transformation: detectTransformation(potentialExpr) };
    }
  }

  const transformation = detectTransformation(trimmed);
  const dotMatch = trimmed.match(/\.(\w+)$/);
  const simpleAlias = dotMatch ? dotMatch[1] : (/^\w+$/.test(trimmed) ? trimmed : '');
  return { expression: trimmed, alias: simpleAlias, originalName: trimmed, displayText: trimmed, transformation };
}

function detectTransformation(expr: string): string {
  const upper = expr.toUpperCase();
  if (/\b(SUM|COUNT|AVG|MAX|MIN|GROUP_CONCAT|STRING_AGG|ARRAY_AGG|LISTAGG|COLLECT_LIST|COLLECT_SET)\s*\(/i.test(upper)) {
    const m = upper.match(/\b(SUM|COUNT|AVG|MAX|MIN|GROUP_CONCAT|STRING_AGG|ARRAY_AGG|LISTAGG|COLLECT_LIST|COLLECT_SET)\s*\(/i);
    return `聚合:${m![1]}`;
  }
  if (/\bOVER\s*\(/i.test(upper)) {
    const fm = upper.match(/\b(ROW_NUMBER|RANK|DENSE_RANK|NTILE|LAG|LEAD|FIRST_VALUE|LAST_VALUE|SUM|COUNT|AVG|MAX|MIN)\s*\([\s\S]*?\)\s*OVER/i);
    return fm ? `开窗:${fm[1]}` : '开窗函数';
  }
  if (/\b(UNNEST|EXPLODE|LATERAL\s+FLATTEN)\s*\(/i.test(upper)) return '炸开(展开)';
  if (/\b(CONCAT|CONCAT_WS)\s*\(/i.test(upper) || /\|\|/.test(expr)) return '拼接';
  if (/\bCASE\b/i.test(upper)) return '条件判断(CASE)';
  if (/\b(CAST|CONVERT|TO_DATE|TO_CHAR|TO_NUMBER|DATE_FORMAT)\s*\(/i.test(upper)) return '类型转换';
  if (/\b(COALESCE|NVL|IFNULL|ISNULL)\s*\(/i.test(upper)) return '空值处理';
  if (/\b(SUBSTR|SUBSTRING|LEFT|RIGHT|TRIM|LTRIM|RTRIM|UPPER|LOWER|REPLACE|REGEXP)\s*\(/i.test(upper)) return '字符串处理';
  if (/(DATEADD|DATEDIFF|DATE_ADD|DATE_SUB|EXTRACT|YEAR|MONTH|DAY)\s*\(/i.test(upper)) return '日期处理';
  if (expr === '*') return '原始字段';
  if (/[+\-*/]/.test(expr) && !/\(/.test(expr)) return '算术运算';
  return '原始字段';
}

function splitTopLevel(str: string, delimiter: string): string[] {
  const result: string[] = [];
  let depth = 0;
  let current = '';
  for (let i = 0; i < str.length; i++) {
    if (str[i] === '(') depth++;
    if (str[i] === ')') depth--;
    if (str[i] === delimiter && depth === 0) { result.push(current); current = ''; }
    else current += str[i];
  }
  if (current.trim()) result.push(current);
  return result;
}

export function parseSQL(sql: string): ParsedSQL {
  const cleaned = removeComments(sql);
  
  // 1. 提取临时表 (CREATE TABLE ... AS ...)
  const { tempTables, remainingSQL: sqlAfterTemp } = preprocessTempTables(cleaned);
  
  const normalized = normalizeWhitespace(sqlAfterTemp);

  const { ctes: rawCTEs, remainingSQL } = extractCTEs(normalized);
  
  const tempTableNames = tempTables.map(t => t.name.toLowerCase());
  const cteNames = rawCTEs.map(c => c.name.toLowerCase());
  
  const ctes: SubQuery[] = [];
  const subQueries: SubQuery[] = [];
  const subQueryCounter = { count: 0 };
  const processedBodies = new Map<string, string>();

  // 2. 处理 Temp Tables (作为特殊的 SubQuery 加入 ctes 列表)
  for (let i = 0; i < tempTables.length; i++) {
    const t = tempTables[i];
    // Temp Table 可以引用之前的 Temp Table
    const knownForTemp = tempTableNames.slice(0, i);
    
    const parsed = processSQL(
      t.body, `temp_${t.name}`, t.name, false, false,
      [...knownForTemp], subQueries, subQueryCounter, processedBodies,
    );
    parsed.isTempTable = true;
    parsed.id = `temp_node_${t.name}`; // 确保 ID 唯一
    ctes.push(parsed);
  }

  // 3. 处理 CTEs
  for (let i = 0; i < rawCTEs.length; i++) {
    const cte = rawCTEs[i];
    const knownForCTE = [...tempTableNames, ...cteNames.slice(0, i)];
    const parsed = processSQL(
      cte.body, `cte_${i}`, cte.name, true, false,
      [...knownForCTE], subQueries, subQueryCounter, processedBodies,
    );
    ctes.push(parsed);
  }

  // 4. 处理 Main Query
  const allKnown = [...tempTableNames, ...cteNames, ...subQueries.map(sq => sq.id.toLowerCase())];
  // 如果 remainingSQL 为空（说明全是 CREATE TABLE 语句），则构造一个空的 Main Query
  const finalSQL = remainingSQL.trim() ? remainingSQL : 'SELECT * FROM ' + (tempTableNames[tempTableNames.length - 1] || 'dual');
  
  const mainQuery = processSQL(
    finalSQL, 'main', '最终查询', false, false,
    allKnown, subQueries, subQueryCounter, processedBodies,
  );

  return {
    ctes,
    mainQuery,
    subQueries,
    allQueries: [...ctes, ...subQueries, mainQuery],
  };
}