import { CustomNode } from '../types';
import { KNOWN_NODE_TYPES } from '../constants';
import { NodeType } from '../enums';

export function addNodesDefaultPosition(nodes: Partial<CustomNode>[]): CustomNode[] {
  for (const node of nodes) {
    node.position = { x: 0, y: 0 }
  }
  return nodes as CustomNode[]
}

export function fixNodeTypes(nodes: Partial<CustomNode>[]): CustomNode[] {
  for (const node of nodes) {
    if (!KNOWN_NODE_TYPES.has(node.type as any)) {
      node.type = NodeType.other
    } else if (node.id!.includes('GenericMap')) {  // FIXME: Удалить после улучшения структуры data.js
      node.type = NodeType.map
    } else if (node.id!.includes('GenericReduce')) { // FIXME: Удалить после улучшения структуры data.js
      node.type = NodeType.reduce
    }
  }
  return nodes as CustomNode[]
}