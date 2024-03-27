import React, { useState } from 'react'

import { Panel } from 'reactflow'
import { Box, Button, DropdownMenu, Flex, TextField } from '@radix-ui/themes'
import { MagnifyingGlassIcon } from '@radix-ui/react-icons';
import { DisplayNameMode } from '../../enums';
import { useStore } from '../../store';
import { NO_SEARCH, SEARCH_DEBOUNCE_MS } from '../../constants';
import { doSearch } from '../../utils/search';
import { useDebounce } from 'react-use';

export interface IControlPanelProps {
  displayNameMode: DisplayNameMode,
  onDisplayNameModeChange: (displayNameMode: DisplayNameMode) => void
}


export const ControlPanel: React.FC<IControlPanelProps> = (
  {
    displayNameMode,
    onDisplayNameModeChange,
  }
) => {
  const setFoundNodeIds = useStore((state) => state.setFoundNodeIds)
  const searchIndex = useStore((state) => state.searchIndex)!

  const [searchTerm, setSearchTerm] = useState<string>('')

  useDebounce(
    () => {
      if (!searchTerm) {
        setFoundNodeIds(NO_SEARCH)
        return
      }

      setFoundNodeIds(doSearch(searchIndex, searchTerm))
    },
    SEARCH_DEBOUNCE_MS,
    [searchTerm]
  );

  return (
    <Panel position="top-right">
      <Flex gap="2">
        <Box width="25em">
          <TextField.Root
            placeholder="Поиск по графу…"
            value={searchTerm}
            onChange={(event) => setSearchTerm(event.target.value)}>
            <TextField.Slot>
              <MagnifyingGlassIcon height="16" width="16"/>
            </TextField.Slot>
          </TextField.Root>
        </Box>
        <DropdownMenu.Root>
          <DropdownMenu.Trigger>
            <Button variant="soft">
              Настройки
              <DropdownMenu.TriggerIcon/>
            </Button>
          </DropdownMenu.Trigger>
          <DropdownMenu.Content>
            <DropdownMenu.Label>Подписи узлов</DropdownMenu.Label>
            <DropdownMenu.RadioGroup
              value={displayNameMode}
              onValueChange={value => onDisplayNameModeChange(value as DisplayNameMode)}
            >
              <DropdownMenu.RadioItem value={DisplayNameMode.id}>
                ID
              </DropdownMenu.RadioItem>
              <DropdownMenu.RadioItem value={DisplayNameMode.name}>
                Техническое имя
              </DropdownMenu.RadioItem>
              <DropdownMenu.RadioItem value={DisplayNameMode.verbose_name}>
                Название
              </DropdownMenu.RadioItem>
            </DropdownMenu.RadioGroup>

          </DropdownMenu.Content>
        </DropdownMenu.Root>
      </Flex>
    </Panel>
  )
}
