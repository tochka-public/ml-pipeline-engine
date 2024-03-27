import { CustomNode } from '../types';
import { useStore } from '../store';
import { Highlight } from '../enums';
import { useNodeId } from 'reactflow';
import { NO_SEARCH } from '../constants';


export function useSelectedNode(): CustomNode | null {
  const selectedNodeId = useStore((state) => state.selectedNodeId)
  const dagreGraph = useStore((state) => state.dagreGraph!)

  return selectedNodeId ? dagreGraph.node(selectedNodeId) as any : null
}

export function useNodeSearchMatch(): { isSearchMode: boolean, isMatch: boolean, isNoMatch: boolean } {
  const nodeId = useNodeId()!
  const foundNodeIDs = useStore((state) => state.foundNodeIDs)

  const isSearchMode = foundNodeIDs !== NO_SEARCH
  const isMatch = isSearchMode && foundNodeIDs.has(nodeId)
  const isNoMatch = isSearchMode && !isMatch

  return { isSearchMode, isMatch, isNoMatch }
}

export function useHighlightMode(): Highlight | null {
  const nodeId = useNodeId()!

  const selectedNodeId = useStore((state) => state.selectedNodeId)
  const selectedNodeDeps = useStore((state) => state.selectedNodeDeps)

  if (nodeId === selectedNodeId) {
    return Highlight.selected
  }

  if (selectedNodeDeps.predcessors.has(nodeId)) {
    return Highlight.predcessor
  }

  if (selectedNodeDeps.successors.has(nodeId)) {
    return Highlight.successor
  }

  return null
}

export function useMute(): { muteNode: boolean, muteMark: boolean } {
  const nodeId = useNodeId()!

  const selectedNodeId = useStore((state) => state.selectedNodeId)

  const highlightMode = useHighlightMode()
  const { isMatch, isNoMatch } = useNodeSearchMatch()

  let [muteNode, muteMark] = [false, false]

  if (isNoMatch) {
    [muteNode, muteMark] = [true, true]
  }

  if (selectedNodeId !== null) {
    [muteNode, muteMark] = [true, true]
  }

  if (selectedNodeId === nodeId) {
    [muteNode, muteMark] = [false, false]
  }

  if (isMatch) {
    [muteNode, muteMark] = [false, false]
  }

  if (highlightMode) {
    [muteNode, muteMark] = [false, true]
  }

  return {
    muteNode,
    muteMark,
  }
}

