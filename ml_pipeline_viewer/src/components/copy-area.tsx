import { IconButton } from '@radix-ui/themes'
import React, { useState } from 'react'
import { CopyIcon } from '@radix-ui/react-icons';
import styled, { css } from 'styled-components';
import { copyTextToClipboard } from '../utils/clipboard';

const Wrapper = styled.div`
  overflow: hidden;
  display: inline-block;
`

const StyledIconButton = styled(IconButton)<{ $isHidden: boolean }>`
  display: inline-block;
  align-self: center;
  margin-left: 0.1em;
  transform: scale(0.8);

  ${props => props.$isHidden && css`
    opacity: 0 !important;
  `}
`

export const CopyArea: React.FC<{ children: any }> = ({ children }) => {
  const [isHidden, setIsHidden] = useState(true)

  return (
    <Wrapper onMouseEnter={() => setIsHidden(false)} onMouseLeave={() => setIsHidden(true)}>
      {children}
      <StyledIconButton
        size="1"
        aria-label="Копировать"
        color="gray"
        variant="ghost"
        radius="medium"
        $isHidden={isHidden}
        onClick={() => copyTextToClipboard(children.props?.href || children)}
      >
        <CopyIcon/>
      </StyledIconButton>
    </Wrapper>
  )
}
