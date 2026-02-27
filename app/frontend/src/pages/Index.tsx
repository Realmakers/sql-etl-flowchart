import { useState, useCallback } from 'react';
import SqlEditor from '@/components/SqlEditor';
import FlowChart from '@/components/FlowChart';
import { parseSQL } from '@/lib/sqlParser';
import { buildFlowElements } from '@/lib/flowLayoutEngine';
import type { Node, Edge } from 'reactflow';
import { toast } from 'sonner';

export default function IndexPage() {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [key, setKey] = useState(0);

  const handleParse = useCallback((sql: string) => {
    setIsLoading(true);
    try {
      const parsed = parseSQL(sql);

      if (parsed.allQueries.length === 0) {
        toast.error('无法解析SQL语句，请检查语法是否正确');
        setIsLoading(false);
        return;
      }

      const { nodes: flowNodes, edges: flowEdges } = buildFlowElements(parsed);
      setNodes(flowNodes);
      setEdges(flowEdges);
      setKey(prev => prev + 1);

      const cteCount = parsed.ctes.length;
      const tableCount = new Set(
        parsed.allQueries.flatMap(q => q.tables.map(t => t.name.toLowerCase()))
      ).size;

      toast.success(
        `解析完成！发现 ${cteCount} 个CTE，${tableCount} 个数据表，已生成ETL流程图`
      );
    } catch (err) {
      console.error('SQL parse error:', err);
      toast.error('SQL解析出错，请检查语句是否完整正确');
    } finally {
      setIsLoading(false);
    }
  }, []);

  return (
    <div className="h-screen w-screen flex flex-col bg-slate-100 overflow-hidden">
      {/* Top Bar */}
      <header className="bg-slate-900 text-white px-6 py-3 flex items-center justify-between border-b border-slate-700 flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="text-2xl">🔄</div>
          <div>
            <h1 className="text-lg font-bold tracking-tight">SQL → ETL 流程图</h1>
            <p className="text-[11px] text-slate-400">将SQL查询语句解析为可视化的数据管道流程图</p>
          </div>
        </div>
        <div className="flex items-center gap-4 text-xs text-slate-400">
          <div className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded bg-blue-400" />
            <span>事实表</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded bg-green-400" />
            <span>维度表</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded bg-purple-400" />
            <span>CTE</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded bg-pink-400" />
            <span>临时表</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 bg-amber-400" style={{ transform: 'rotate(45deg)', borderRadius: 2 }} />
            <span>JOIN</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded bg-slate-400" />
            <span>源表</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded bg-emerald-400" />
            <span>输出</span>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left Panel - SQL Editor */}
        <div className="w-[420px] flex-shrink-0 border-r border-slate-300">
          <SqlEditor onParse={handleParse} isLoading={isLoading} />
        </div>

        {/* Right Panel - Flow Chart */}
        <div className="flex-1 relative">
          <FlowChart key={key} nodes={nodes} edges={edges} />
        </div>
      </div>
    </div>
  );
}