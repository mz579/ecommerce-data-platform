/** 按需注册 ECharts 模块，避免打包完整 echarts。 */
import { use } from 'echarts/core'
import { BarChart, FunnelChart, HeatmapChart, LineChart, ScatterChart } from 'echarts/charts'
import {
  AxisPointerComponent,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  TooltipComponent,
  VisualMapComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

use([
  LineChart,
  BarChart,
  HeatmapChart,
  ScatterChart,
  FunnelChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  AxisPointerComponent,
  MarkLineComponent,
  VisualMapComponent,
  CanvasRenderer,
])
