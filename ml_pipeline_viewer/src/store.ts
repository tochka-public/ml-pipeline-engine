import { create } from 'zustand'
import { IGraphData } from './types';
import { NO_SEARCH } from './constants';
import Dagre from '@dagrejs/dagre';
import { buildGraph, getNodeDeps, INodeDeps } from './utils/dagreGraph';
import { buildSearchIndex } from './utils/search';
import { devtools } from 'zustand/middleware';


export interface IStore {
  graphData: IGraphData | null
  dagreGraph: Dagre.graphlib.Graph | null
  searchIndex: { [k: string]: string } | null

  selectedNodeId: string | null
  selectedNodeDeps: INodeDeps
  foundNodeIDs: Set<string> | typeof NO_SEARCH

  setGraphData: (graphData: IGraphData) => void
  setSelectedNodeId: (nodeId: string | null) => void
  setFoundNodeIds: (nodeIds: Set<string> | string[] | typeof NO_SEARCH) => void
}

export const useStore = create<IStore, any>(
  devtools((set) => ({
    graphData: null,
    dagreGraph: null,
    searchIndex: null,

    selectedNodeId: null,
    selectedNodeDeps: {
      predcessors: new Set(),
      successors: new Set()
    },
    foundNodeIDs: NO_SEARCH,

    setGraphData: (graphData) => set(state => ({
      ...state,
      graphData,
      dagreGraph: buildGraph(graphData.nodes, graphData.edges),
      searchIndex: buildSearchIndex(graphData!.nodes),
    })),

    setSelectedNodeId: (nodeId) => set(state => ({
      ...state,
      selectedNodeId: nodeId,
      selectedNodeDeps: getNodeDeps(state.dagreGraph!, nodeId),
    })),

    setFoundNodeIds: (nodeIds) => set(state => ({
      ...state,
      foundNodeIDs: nodeIds === NO_SEARCH ? nodeIds : new Set(nodeIds),
    })),
  }))
)