import React from 'react'

import { Panel, useReactFlow } from 'reactflow'
import { Button, Flex } from '@radix-ui/themes'
import { EnterFullScreenIcon, MinusIcon, PlusIcon } from '@radix-ui/react-icons';
import styled from 'styled-components';


const RoundButton = styled(Button)`
  width: 2.7em;
  height: 2.7em;
  cursor: pointer;
`

export const ZoomPanel: React.FC = () => {
  const { fitView, zoomIn, zoomOut } = useReactFlow()

  return (
    <Panel position="top-right">
      <Flex direction="column" mt="9" gap="2">
        <RoundButton variant="soft" onClick={() => zoomIn()}><PlusIcon/></RoundButton>
        <RoundButton variant="soft" onClick={() => zoomOut()}><MinusIcon/></RoundButton>
        <RoundButton variant="soft" onClick={() => fitView()}><EnterFullScreenIcon/></RoundButton>
      </Flex>
    </Panel>
  )
}
