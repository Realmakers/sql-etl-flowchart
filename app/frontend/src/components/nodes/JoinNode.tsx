import { memo } from 'react';
import { Handle, Position } from 'reactflow';
import type { JoinNodeData } from '@/lib/flowLayoutEngine';

function JoinNode({ data }: { data: JoinNodeData }) {
  return (
    <div className="relative flex items-center justify-center" style={{ width: 100, height: 100 }}>
      <Handle type="target" position={Position.Top} className="!bg-amber-500 !w-3 !h-3 !top-0" />

      {/* Diamond shape */}
      <div
        className="absolute bg-gradient-to-br from-amber-400 to-amber-500 shadow-lg border-2 border-amber-600 flex items-center justify-center"
        style={{
          width: 70,
          height: 70,
          transform: 'rotate(45deg)',
          borderRadius: 8,
        }}
      >
        <div
          className="text-center"
          style={{ transform: 'rotate(-45deg)' }}
        >
          <div className="text-[10px] font-bold text-white leading-tight whitespace-nowrap">
            {data.joinType}
          </div>
        </div>
      </div>

      {/* Condition tooltip below diamond */}
      {data.condition && (
        <div className="absolute -bottom-6 left-1/2 -translate-x-1/2 bg-amber-900 text-amber-100 text-[9px] px-2 py-0.5 rounded whitespace-nowrap max-w-[200px] truncate z-10 shadow">
          {data.condition}
        </div>
      )}

      <Handle type="source" position={Position.Bottom} className="!bg-amber-500 !w-3 !h-3 !bottom-0" />
    </div>
  );
}

export default memo(JoinNode);