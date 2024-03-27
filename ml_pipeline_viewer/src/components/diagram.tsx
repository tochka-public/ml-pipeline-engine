import React, { useCallback, useState } from 'react'
import ReactFlow, {
  Background,
  MiniMap,
  useEdgesState,
  useNodesState,
  useOnSelectionChange,
  useReactFlow
} from 'reactflow'

import 'reactflow/dist/style.css'
import { DisplayNameMode } from '../enums'
import { CustomNode } from '../types';
import { ControlPanel } from './panels/control-panel';
import { NODE_TYPES } from '../constants';
import { changeNodesDisplayName, getLayoutedElements } from '../utils/diagram';
import { ZoomPanel } from './panels/zoom-panel';
import { useStore } from '../store';
import styled, { css } from 'styled-components';

export interface IDiargamProps {
  initialDisplayNameMode: DisplayNameMode
}

const Wrapper = styled.div<{ $show: boolean }>`
  flex: 1;
  opacity: 0;

  ${(props) => props.$show && css`
    transition: opacity 0.3s;
    opacity: 1;
  `}
`

export const Diagram: React.FC<IDiargamProps> = (
  {
    initialDisplayNameMode,
  }
) => {
  const { fitView } = useReactFlow()

  const graphData = useStore((state) => state.graphData)
  const setSelectedNodeId = useStore((state) => state.setSelectedNodeId)

  const [nodes, setNodes, onNodesChange] = useNodesState(graphData!.nodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(graphData!.edges)

  const [showDiagram, setShowDiagram] = useState(false)
  const [displayNameMode, setDisplayNameMode] = useState<DisplayNameMode>(initialDisplayNameMode)

  useOnSelectionChange({
    onChange: ({ nodes }) => setSelectedNodeId(nodes.length ? nodes[0].id : null),
  })

  const layoutElements = useCallback(() => {
    const layouted = getLayoutedElements(nodes as CustomNode[], edges)

    setNodes([...layouted.nodes])
    setEdges([...layouted.edges])

    setTimeout(() => {
      window.requestAnimationFrame(() => {
        fitView()
        setShowDiagram(true)
      })
    }, 200)
  }, [nodes, edges, setNodes, setEdges, fitView])

  const onDisplayNameModeChange = useCallback((displayNameMode: DisplayNameMode) => {
    setNodes(changeNodesDisplayName(nodes as CustomNode[], displayNameMode))
    setDisplayNameMode(displayNameMode)
  }, [setNodes, nodes])


  return (
    <Wrapper $show={showDiagram}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onInit={layoutElements}
        minZoom={0.1}
        nodesDraggable={false}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        elementsSelectable={true}
        edgesFocusable={false}
        nodeTypes={NODE_TYPES}
      >
        <ZoomPanel/>
        <ControlPanel displayNameMode={displayNameMode} onDisplayNameModeChange={onDisplayNameModeChange}/>
        <MiniMap position="bottom-left" zoomable pannable/>
        <Background color="#aaa" gap={20}/>
      </ReactFlow>
    </Wrapper>
  )
}
