# SQL to ETL Flowchart Web Application

## Design Guidelines

### Design References
- **Modern data engineering tools**: dbt, Airflow, Dagster UI aesthetics
- **Style**: Clean, professional, dark-accented data tool interface

### Color Palette
- Primary Background: #0F172A (Slate 900)
- Secondary Background: #1E293B (Slate 800)
- Card Background: #FFFFFF (White)
- Accent Blue: #3B82F6 (for fact tables & primary actions)
- Accent Green: #10B981 (for dimension tables)
- Accent Amber: #F59E0B (for JOIN diamonds)
- Accent Purple: #8B5CF6 (for CTE nodes)
- Text Primary: #1E293B
- Text Secondary: #64748B

### Typography
- Headings: Inter, font-weight 700
- Body: Inter, font-weight 400
- Code: JetBrains Mono / monospace

### Key Component Styles
- SQL Editor: Dark background with monospace font, syntax-like styling
- Flow nodes: Rounded rectangles with colored borders
- JOIN diamonds: Rotated squares with amber background
- Fact tables: Blue-bordered small rectangles inside nodes
- Dimension tables: Green-bordered small rectangles inside nodes

---

## Development Tasks

### Files to Create (8 files max):

1. **src/pages/Index.tsx** - Main page layout: left SQL editor panel + right flowchart panel
2. **src/lib/sqlParser.ts** - SQL parsing engine: parse CTE, subqueries, tables, fields, filters, JOINs, transformations
3. **src/components/SqlEditor.tsx** - SQL input textarea with example button and parse button
4. **src/components/FlowChart.tsx** - ReactFlow-based ETL flowchart visualization
5. **src/components/nodes/QueryNode.tsx** - Custom ReactFlow node for subquery/CTE rectangles
6. **src/components/nodes/JoinNode.tsx** - Custom ReactFlow diamond node for JOIN relationships
7. **src/lib/flowLayoutEngine.ts** - Convert parsed SQL structure into ReactFlow nodes & edges with auto-layout
8. **index.html** - Update title

### Implementation Notes:
- Use `reactflow` for flowchart rendering
- SQL parsing will be a custom regex/state-machine based parser (no external SQL parser library needed for MVP)
- Focus on common SQL patterns: WITH CTE, SELECT, FROM, JOIN, WHERE, GROUP BY, window functions
- Fact/dimension table detection by naming convention (fact_, dim_, f_, d_ prefixes)
- Auto-layout using dagre for hierarchical positioning