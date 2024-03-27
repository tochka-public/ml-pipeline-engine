import { CustomNode } from '../types';
import { Edge } from 'reactflow';
import Dagre from '@dagrejs/dagre';

export interface INodeDeps {
  predcessors: Set<string>
  successors: Set<string>
}


export function buildGraph(nodes: CustomNode[], edges: Edge[]) {
  const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}))

  edges.forEach((edge) => g.setEdge(edge.source, edge.target))
  nodes.forEach((node) => g.setNode(node.id, node as any))

  return g
}


export function getNodeDeps(graph: Dagre.graphlib.Graph, nodeId: string | null): INodeDeps {
  if (!nodeId) {
    return {
      predcessors: new Set(),
      successors: new Set(),
    }
  }

  return {
    predcessors: new Set(graph.predecessors(nodeId) as any),
    successors: new Set(graph.successors(nodeId) as any),
  }
}
