import { CustomNode, SearchIndex } from '../types';

export function buildSearchIndex(nodes: CustomNode[]) {
  const searchIndex: SearchIndex = {}

  for (const node of nodes) {
    searchIndex[node.id] = node.id

    searchIndex[node.data.name] = node.id

    if (node.data.verbose_name) {
      searchIndex[node.data.verbose_name] = node.id
    }
  }

  return searchIndex
}

export function doSearch(searchIndex: SearchIndex, term: string) {
  const foundNodeIDs: string[] = []
  for (const str of Array.from(Object.keys(searchIndex))) {
    if (str.toLowerCase().includes(term.toLowerCase())) {
      foundNodeIDs.push(searchIndex[str])
    }
  }

  return foundNodeIDs
}