import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';

const EXAMPLE_SQL = `WITH customer_orders AS (
  SELECT
    c.customer_id,
    c.customer_name,
    c.region,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount) AS total_amount,
    AVG(o.amount) AS avg_order_amount
  FROM dim_customer c
  LEFT JOIN fact_orders o
    ON c.customer_id = o.customer_id
  WHERE o.order_date >= '2024-01-01'
    AND o.status = 'completed'
  GROUP BY c.customer_id, c.customer_name, c.region
),
product_sales AS (
  SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(oi.quantity) AS total_quantity,
    SUM(oi.quantity * oi.unit_price) AS revenue,
    ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS rank_in_category
  FROM dim_product p
  INNER JOIN fact_order_items oi
    ON p.product_id = oi.product_id
  INNER JOIN fact_orders o
    ON oi.order_id = o.order_id
  WHERE o.order_date >= '2024-01-01'
  GROUP BY p.product_id, p.product_name, p.category
),
regional_summary AS (
  SELECT
    co.region,
    SUM(co.total_amount) AS region_revenue,
    COUNT(co.customer_id) AS customer_count,
    CONCAT(co.region, '_', CAST(SUM(co.total_amount) AS VARCHAR)) AS region_label
  FROM customer_orders co
  GROUP BY co.region
  HAVING SUM(co.total_amount) > 10000
)
SELECT
  rs.region,
  rs.region_revenue,
  rs.customer_count,
  rs.region_label,
  ps.product_name AS top_product,
  ps.revenue AS top_product_revenue,
  CASE
    WHEN rs.region_revenue > 100000 THEN '高价值区域'
    WHEN rs.region_revenue > 50000 THEN '中价值区域'
    ELSE '低价值区域'
  END AS region_tier
FROM regional_summary rs
LEFT JOIN product_sales ps
  ON ps.rank_in_category = 1
ORDER BY rs.region_revenue DESC`;

interface SqlEditorProps {
  onParse: (sql: string) => void;
  isLoading?: boolean;
}

export default function SqlEditor({ onParse, isLoading }: SqlEditorProps) {
  const [sql, setSql] = useState('');

  const handleParse = () => {
    if (sql.trim()) {
      onParse(sql.trim());
    }
  };

  const handleLoadExample = () => {
    setSql(EXAMPLE_SQL);
  };

  return (
    <div className="flex flex-col h-full bg-slate-900 text-white">
      {/* Header */}
      <div className="px-4 py-3 border-b border-slate-700 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
          <h2 className="text-sm font-bold tracking-wide">SQL 输入</h2>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={handleLoadExample}
          className="text-xs bg-slate-800 border-slate-600 text-slate-300 hover:bg-slate-700 hover:text-white"
        >
          📋 加载示例SQL
        </Button>
      </div>

      {/* Editor Area */}
      <div className="flex-1 p-3 overflow-hidden">
        <Textarea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          placeholder={`在此输入完整的SQL查询语句...

支持解析：
• WITH 公共表达式 (CTE)
• 多表 JOIN 关联
• 聚合函数 (SUM, COUNT, AVG...)
• 开窗函数 (ROW_NUMBER, RANK...)
• 过滤条件 (WHERE, HAVING)
• 字段加工逻辑`}
          className="h-full w-full resize-none bg-slate-800 border-slate-700 text-slate-100 font-mono text-sm leading-relaxed placeholder:text-slate-500 focus:border-blue-500 focus:ring-blue-500/20 rounded-lg"
          spellCheck={false}
        />
      </div>

      {/* Footer */}
      <div className="px-4 py-3 border-t border-slate-700 flex items-center justify-between">
        <div className="text-xs text-slate-500">
          {sql.trim() ? `${sql.split('\n').length} 行` : '等待输入...'}
        </div>
        <Button
          onClick={handleParse}
          disabled={!sql.trim() || isLoading}
          className="bg-blue-600 hover:bg-blue-700 text-white font-semibold px-6"
        >
          {isLoading ? (
            <>
              <span className="animate-spin mr-2">⚙️</span>
              解析中...
            </>
          ) : (
            <>🚀 解析并生成流程图</>
          )}
        </Button>
      </div>
    </div>
  );
}