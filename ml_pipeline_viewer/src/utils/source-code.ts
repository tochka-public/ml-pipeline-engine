import { DEFAULT_GIT_TREE } from '../constants';

export function getFullRepoLink(baseRepoLink: string, relativeRepoLink: string): string {
  return `${baseRepoLink}${DEFAULT_GIT_TREE}/${relativeRepoLink}`
}