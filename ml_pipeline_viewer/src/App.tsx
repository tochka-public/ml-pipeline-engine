import React from 'react'
import { ReactFlowProvider } from 'reactflow'

import { Diagram } from './components/diagram';
import { GlobalStyle } from './theme';
import { changeNodesDisplayName } from './utils/diagram';
import { Theme } from '@radix-ui/themes';
import { IGraphData } from './types';
import { useStore } from './store';
import { DetailsDrawer } from './components/details-drawer';
import styled from 'styled-components';
import { INITIAL_DISPLAY_MODE } from './constants';
import { addNodesDefaultPosition, fixNodeTypes } from './utils/graph-data';

const GRAPH_DATA = window.__GRAPH_DATA__ as IGraphData

const Wrapper = styled.div`
  height: 100%;
  display: flex;
`

export const App: React.FC = () => {
  const setGraphData = useStore((state) => state.setGraphData)

  setGraphData({
    ...GRAPH_DATA,
    nodes: changeNodesDisplayName(addNodesDefaultPosition(fixNodeTypes(GRAPH_DATA.nodes)), INITIAL_DISPLAY_MODE)
  })

  return (
    <>
      <GlobalStyle/>
      <Theme accentColor="gray" radius="full" scaling="95%">
        <ReactFlowProvider>
          <Wrapper>
            <Diagram initialDisplayNameMode={INITIAL_DISPLAY_MODE}/>
            <DetailsDrawer/>
          </Wrapper>
        </ReactFlowProvider>
      </Theme>
    </>
  )
}
