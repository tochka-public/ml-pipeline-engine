import { Edge } from 'reactflow'
import { CustomNode } from '../types'
import Dagre from '@dagrejs/dagre'
import { DisplayNameMode } from '../enums'
import { buildGraph } from './dagreGraph'

export function changeNodesDisplayName(nodes: CustomNode[], mode: DisplayNameMode): CustomNode[] {
  return nodes.map(node => {
    let display_name = node.id
    let is_display_name_fallback = false

    if (mode === DisplayNameMode.name) {
      if (node.data.name) {
        display_name = node.data.name
      } else {
        is_display_name_fallback = true
      }

    } else if (mode === DisplayNameMode.verbose_name) {
      if (node.data.verbose_name) {
        display_name = node.data.verbose_name
      } else {
        is_display_name_fallback = true
      }
    }

    return { ...node, data: { ...node.data, display_name, is_display_name_fallback } }
  })
}


export function getLayoutedElements(nodes: CustomNode[], edges: Edge[], nodesep = 60, ranksep = 700) {
  const g = buildGraph(nodes, edges)

  g.setGraph({
    rankdir: 'LR',
    ranker: 'longest-path',
    nodesep,
    ranksep,
  })

  edges.forEach((edge) => g.setEdge(edge.source, edge.target))
  nodes.forEach((node) => g.setNode(node.id, node as any))

  Dagre.layout(g)

  return {
    nodes: nodes.map((node) => {
      const { x, y } = g.node(node.id)
      return { ...node, position: { x, y } }
    }),
    edges,
  }
}
