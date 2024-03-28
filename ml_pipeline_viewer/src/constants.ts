import { DisplayNameMode, NodeType } from './enums';
import { GenericNode, MapNode, ProcessorNode, ReduceNode, SwitchNode } from './components/node';
import { NodeTypeArrtibutes } from './types';

export const SEARCH_DEBOUNCE_MS = 100

export const NO_SEARCH = Symbol()

export const DEFAULT_GIT_TREE = '/-/tree/master'

export const INITIAL_DISPLAY_MODE = DisplayNameMode.id

export const NODE_TYPES = {
  [NodeType.datasource]: ProcessorNode,
  [NodeType.feature]: ProcessorNode,
  [NodeType.ml_model]: ProcessorNode,
  [NodeType.processor]: ProcessorNode,
  [NodeType.switch]: SwitchNode,
  [NodeType.map]: MapNode,
  [NodeType.reduce]: ReduceNode,
  [NodeType.other]: GenericNode,
}

export const KNOWN_NODE_TYPES = new Set(Object.keys(NODE_TYPES))

export const NODE_TYPE_ATTRIBUTES: NodeTypeArrtibutes = {
  [NodeType.datasource]: {
    'verbose_name': 'DataSource',
    'abbr': 'DS',
    'hex_bgr_color': '#d56cff',
  },
  [NodeType.feature]: {
    'verbose_name': 'Feature',
    'abbr': 'FT',
    'hex_bgr_color': '#ffd42d',
  },
  [NodeType.ml_model]: {
    'verbose_name': 'ML Model',
    'abbr': 'ML',
    'hex_bgr_color': '#83ffba',
  },
  [NodeType.processor]: {
    'verbose_name': 'Processor',
    'abbr': 'PR',
    'hex_bgr_color': '#5cd4ef',
  },
  [NodeType.switch]: {
    'verbose_name': 'Switch',
    'abbr': 'SW',
    'hex_bgr_color': '#ff8773'
  },
  [NodeType.map]: {
    'verbose_name': 'Map',
    'abbr': 'MAP',
    'hex_bgr_color': '#9bf865'
  },
  [NodeType.reduce]: {
    'verbose_name': 'Reduce',
    'abbr': 'RDC',
    'hex_bgr_color': '#9bf865'
  },
  [NodeType.other]: {
    'verbose_name': 'Other',
    'abbr': '?',
    'hex_bgr_color': '#b0b0b0'
  }
}