import { memo } from 'react';
import { Handle, Position } from 'reactflow';
import type { QueryNodeData } from '@/lib/flowLayoutEngine';

const nodeTypeStyles: Record<string, { border: string; bg: string; headerBg: string; headerText: string; icon: string; handleColor: string }> = {
  source: {
    border: 'border-slate-400',
    bg: 'bg-slate-50',
    headerBg: 'bg-slate-600',
    headerText: 'text-white',
    icon: '📦',
    handleColor: '!bg-slate-500',
  },
  cte: {
    border: 'border-purple-400',
    bg: 'bg-purple-50',
    headerBg: 'bg-purple-600',
    headerText: 'text-white',
    icon: '🔄',
    handleColor: '!bg-purple-500',
  },
  subquery: {
    border: 'border-teal-400',
    bg: 'bg-teal-50',
    headerBg: 'bg-teal-600',
    headerText: 'text-white',
    icon: '📋',
    handleColor: '!bg-teal-500',
  },
  main: {
    border: 'border-blue-400',
    bg: 'bg-blue-50',
    headerBg: 'bg-blue-600',
    headerText: 'text-white',
    icon: '🎯',
    handleColor: '!bg-blue-500',
  },
  output: {
    border: 'border-emerald-400',
    bg: 'bg-emerald-50',
    headerBg: 'bg-emerald-600',
    headerText: 'text-white',
    icon: '✅',
    handleColor: '!bg-emerald-500',
  },
  temp: {
    border: 'border-pink-400',
    bg: 'bg-pink-50',
    headerBg: 'bg-pink-600',
    headerText: 'text-white',
    icon: '🗂️',
    handleColor: '!bg-pink-500',
  },
};

const tableTypeColors: Record<string, { bg: string; border: string; text: string }> = {
  fact: { bg: 'bg-blue-100', border: 'border-blue-400', text: 'text-blue-800' },
  dimension: { bg: 'bg-green-100', border: 'border-green-400', text: 'text-green-800' },
  default: { bg: 'bg-slate-100', border: 'border-slate-300', text: 'text-slate-700' },
};

function getNodeTypeLabel(nodeType: string): string {
  switch (nodeType) {
    case 'cte': return 'CTE';
    case 'subquery': return '子查询';
    case 'main': return '主查询';
    case 'source': return '源表';
    case 'output': return '终点';
    case 'temp': return '临时表';
    default: return '';
  }
}

function QueryNode({ data }: { data: QueryNodeData }) {
  const style = nodeTypeStyles[data.nodeType] || nodeTypeStyles.cte;
  const sq = data.subQuery;
  const isSource = data.nodeType === 'source';
  const isContentNode = data.nodeType === 'cte' || data.nodeType === 'subquery' || data.nodeType === 'main' || data.nodeType === 'temp';

  return (
    <div className={`rounded-xl border-2 ${style.border} ${style.bg} shadow-lg min-w-[280px] max-w-[420px] overflow-hidden`}>
      {/* Input Handle (Top) */}
      <Handle
        type="target"
        position={Position.Top}
        className={`${style.handleColor} !w-3.5 !h-3.5 !border-2 !border-white`}
        title="输入端：接收上游数据"
      />

      {/* Header */}
      <div className={`${style.headerBg} ${style.headerText} px-4 py-2.5 font-bold text-sm flex items-center gap-2`}>
        <span>{style.icon}</span>
        <span className="truncate">{data.label}</span>
        <span className="ml-auto text-xs bg-white/20 px-2 py-0.5 rounded-full">
          {getNodeTypeLabel(data.nodeType)}
        </span>
      </div>

      <div className="p-3 space-y-2.5 text-xs">
        {/* UNION ALL indicator */}
        {sq.unionInfo && (
          <div className="bg-amber-50 border border-amber-300 rounded-md px-2.5 py-1.5 text-amber-800 font-semibold text-[11px] flex items-center gap-1.5">
            <span className="text-amber-500">⚡</span>
            <span>{sq.unionInfo.type}</span>
            <span className="text-[10px] font-normal opacity-80">
              ({sq.unionInfo.sources.join(' + ')})
            </span>
          </div>
        )}

        {/* Source node: show tableName in content area */}
        {isSource && sq.tables.length > 0 && (
          <div>
            <div className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider mb-1.5">数据表</div>
            <div className="space-y-1.5">
              {sq.tables.map((t, i) => {
                const tc = t.type ? tableTypeColors[t.type] : tableTypeColors.default;
                return (
                  <div
                    key={i}
                    className={`${tc.bg} ${tc.border} ${tc.text} border rounded-md px-2.5 py-1.5 font-mono text-[11px]`}
                  >
                    <div className="flex items-center gap-1.5">
                      <span className="font-bold break-all">{t.tableName}</span>
                      {t.alias !== t.name && t.alias !== t.tableName && (
                        <span className="text-[10px] opacity-70">({t.alias})</span>
                      )}
                    </div>
                    {t.type && (
                      <div className="flex items-center gap-1.5 mt-1">
                        <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-semibold ${
                          t.type === 'fact' ? 'bg-blue-200 text-blue-900' :
                          'bg-green-200 text-green-900'
                        }`}>
                          {t.type === 'fact' ? '事实表' : '维度表'}
                        </span>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Content nodes (CTE / Subquery / Main): show tables, fields, filters, group by */}
        {isContentNode && (
          <div>
            <div className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider mb-1.5">数据表</div>
            {sq.tables.length > 0 ? (
              <div className="flex flex-wrap gap-1.5">
                {sq.tables.map((t, i) => {
                  const tc = t.type ? tableTypeColors[t.type] : tableTypeColors.default;
                  return (
                    <div
                      key={i}
                      className={`${tc.bg} ${tc.border} ${tc.text} border rounded-md px-2.5 py-1 font-mono text-[11px] flex flex-col gap-0.5`}
                    >
                      <div className="flex items-center gap-1.5 flex-wrap">
                        <span className="font-bold break-all">{t.name}</span>
                        {t.alias !== t.name && t.alias !== t.tableName && (
                          <span className="text-[10px] opacity-70">({t.alias})</span>
                        )}
                        {t.type && (
                          <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-semibold ${
                            t.type === 'fact' ? 'bg-blue-200 text-blue-900' :
                            'bg-green-200 text-green-900'
                          }`}>
                            {t.type === 'fact' ? '事实表' : '维度表'}
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="text-slate-400 italic text-[10px]">无输入表</div>
            )}
          </div>
        )}

        {/* Fields - showing original name + alias */}
        {/* Rule (5): Always show Fields section for CTE/Subquery even if empty */}
        {(sq.fields.length > 0 || isContentNode) && (
          <div>
            <div className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider mb-1.5">
              字段 ({sq.fields.length})
            </div>
            {sq.fields.length > 0 ? (
              <div className="space-y-1 max-h-[160px] overflow-y-auto pr-1">
                {sq.fields.map((f, i) => (
                  <div key={i} className="flex items-start gap-1.5 bg-white/60 rounded px-2 py-1 border border-slate-200">
                    <span className="font-mono text-slate-700 flex-1 break-all text-[10px] leading-relaxed" title={f.displayText}>
                      {f.displayText.length > 60 ? f.displayText.substring(0, 60) + '...' : f.displayText}
                    </span>
                    {f.transformation !== '原始字段' && (
                      <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-amber-100 text-amber-800 border border-amber-200 whitespace-nowrap flex-shrink-0">
                        {f.transformation}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-slate-400 italic text-[10px]">无字段信息</div>
            )}
          </div>
        )}

        {/* Filters */}
        {sq.filters.length > 0 && (
          <div>
            <div className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider mb-1.5">过滤条件</div>
            <div className="max-h-[120px] overflow-y-auto pr-1">
              {sq.filters.map((f, i) => (
                <div key={i} className="bg-red-50 border border-red-200 rounded px-2 py-1 text-red-800 font-mono mb-1">
                  <span className="font-bold text-red-600">{f.clause}: </span>
                  <span className="break-all">{f.condition}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Group By */}
        {sq.groupBy.length > 0 && (
          <div className="bg-indigo-50 border border-indigo-200 rounded px-2 py-1 text-indigo-800 font-mono">
            <span className="font-bold text-indigo-600">GROUP BY: </span>
            <div className="max-h-[80px] overflow-y-auto pr-1 inline-block w-full align-top">
                {sq.groupBy.join(', ')}
            </div>
          </div>
        )}
      </div>

      {/* Output Handle (Bottom) */}
      <Handle
        type="source"
        position={Position.Bottom}
        className={`${style.handleColor} !w-3.5 !h-3.5 !border-2 !border-white`}
        title="输出端：输出查询结果"
      />
    </div>
  );
}

export default memo(QueryNode);