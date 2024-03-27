import { Handle, NodeProps, Position } from 'reactflow';
import React from 'react';
import styled, { css } from 'styled-components';
import { INodeData } from '../types';
import { ContextMenu } from '@radix-ui/themes';
import { copyTextToClipboard } from '../utils/clipboard';
import { Highlight } from '../enums';
import { useHighlightMode, useMute, useNodeSearchMatch } from '../hooks/diagram';
import { NODE_TYPE_ATTRIBUTES } from '../constants';
import { useStore } from '../store';
import { getFullRepoLink } from '../utils/source-code';

const Wrapper = styled.div<{ muted: boolean }>`
  ${(props) => props.muted && css`
    opacity: 0.5;
  `}
`

const Mark = styled.div<{ $color: string, $mute: boolean }>`
  padding: 0.5em 0.5em 0.5em 0.7em;
  width: 2.5em;
  text-align: center;

  background-color: ${props => props.$color};

  ${(props) => props.$mute && css`
    background-color: rgba(0, 0, 0, 0.05) !important;
  `}
`;

const Label = styled.div<{ $fallback: boolean }>`
  padding: 0.5em 0.5em 0.5em 0.5em;
  max-width: 20em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;

  ${(props) => props.$fallback && css`
    color: color(display-p3 0.553 0.553 0.553);
  `}
`;


const AquaEffect = styled.div`
  background: linear-gradient(rgba(255, 255, 255, 0.6) 0%, rgba(255, 255, 255, 0) 100%);
  content: '';
  top: 0;
  height: 50%;
  position: absolute;
  border-top-left-radius: 2em;
  border-top-right-radius: 2em;
  width: 100%;
`

const InnerWrapper = styled.div<{ $highlightMode: Highlight | null, $isMatch: boolean }>`
  display: flex;
  align-items: center;
  border-radius: 100px;
  padding-right: 0.6em;
  overflow: hidden;
  cursor: pointer;
  opacity: 1;
  background-color: color(display-p3 0.88 0.88 0.88);

  ${(props) => props.$highlightMode === Highlight.selected && css`
    background-color: rgb(155, 248, 101);
    box-shadow: 0 2px 10px 2px rgba(155, 248, 101, 0.6);
  `}

  ${(props) => props.$isMatch && css`
    box-shadow: 0 0 5px 6px rgba(255, 227, 57, 0.8);
  `}

  ${(props) => (props.$highlightMode === Highlight.predcessor || props.$highlightMode === Highlight.successor) && css`
    background-color: rgb(70, 216, 236);
    box-shadow: 0 2px 10px 2px rgba(70, 216, 236, 0.6);
  `}
`;

export const GenericNode: React.FC<NodeProps<INodeData>> = ({ id, type, data }) => {
  const highlightMode = useHighlightMode()
  const { isMatch } = useNodeSearchMatch()
  const { muteNode, muteMark } = useMute()

  const graphData = useStore((state) => state.graphData!)

  const nodeTypeAttrs = NODE_TYPE_ATTRIBUTES[type]

  return (
    <Wrapper muted={muteNode}>
      <ContextMenu.Root>
        <ContextMenu.Trigger>
          <InnerWrapper $highlightMode={highlightMode} $isMatch={isMatch}>
            <Mark $color={nodeTypeAttrs.hex_bgr_color} $mute={muteMark}>{nodeTypeAttrs.abbr}</Mark>
            <Label $fallback={data.is_display_name_fallback!}>
              {data.display_name!}
            </Label>
            <AquaEffect/>
          </InnerWrapper>
        </ContextMenu.Trigger>
        <ContextMenu.Content>
          <ContextMenu.Item onSelect={() => copyTextToClipboard(id)}>Копировать ID</ContextMenu.Item>
          {/*<ContextMenu.Item>Копировать ссылку</ContextMenu.Item>*/}
          <ContextMenu.Separator/>
          <ContextMenu.Item disabled={!graphData.attributes.repo_link} onSelect={() => window.open(
            getFullRepoLink(graphData.attributes.repo_link, data.code_source), '_blank'
          )}>
            Открыть в репозитории
          </ContextMenu.Item>
        </ContextMenu.Content>
      </ContextMenu.Root>

      <Handle type="target" position={Position.Left}/>
      <Handle type="source" position={Position.Right}/>
    </Wrapper>
  )
}

export const ProcessorNode: React.FC<NodeProps<INodeData>> = (props) => {
  return (
    <GenericNode {...props}/>
  )
}

export const SwitchNode: React.FC<NodeProps<INodeData>> = (props) => {
  return (
    <GenericNode {...props}/>
  )
}

export const MapNode: React.FC<NodeProps<INodeData>> = (props) => {
  return (
    <GenericNode {...props}/>
  )
}

export const ReduceNode: React.FC<NodeProps<INodeData>> = (props) => {
  return (
    <GenericNode {...props}/>
  )
}