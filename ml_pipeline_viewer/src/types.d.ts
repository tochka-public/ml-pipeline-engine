import { Node, Edge } from 'reactflow';
import { DisplayNameMode, NodeType } from './enums';

export interface INodeData {
  name: string
  verbose_name: string
  display_name?: string
  is_display_name_fallback?: boolean
  is_virtual: boolean
  is_generic: boolean
  doc: string
  code_source: string
}

export type CustomNode = Node<INodeData, NodeType>

export type NodeTypeArrtibutes = {
  [k: string]: {
    verbose_name: string
    abbr: string
    hex_bgr_color: string
  }
}

export interface IGraphAttributes {
  name: string,
  verbose_name: string,
  repo_link: string,
  nodesep: number,
  ranksep: number,
}

export interface IGraphData {
  name: string
  verbose_name: string
  displayNameMode: DisplayNameMode,
  nodes: CustomNode[]
  edges: Edge[]
  node_types: NodeTypeArrtibutes
  attributes: IGraphAttributes
}

export type SearchIndex = { [k: string]: string }

declare global {
  interface Window {
    __GRAPH_DATA__: Partial<IGraphData>;
  }
}
