import React from 'react'

import { Box, Heading, DataList, Link, Callout } from '@radix-ui/themes'
import { useStore } from '../store';
import styled from 'styled-components';
import { InfoCircledIcon } from '@radix-ui/react-icons';
import { CustomNode, IGraphData } from '../types';
import Markdown from 'react-markdown';
import { CopyArea } from './copy-area';
import { useSelectedNode } from '../hooks/diagram';
import { NODE_TYPE_ATTRIBUTES } from '../constants';
import { getFullRepoLink } from '../utils/source-code';

const Wrapper = styled.div`
  background-color: color(display-p3 0 0 0 / 0.063);
  flex: 0 0 25em;
  width: 25em;
  overflow: hidden;
  overflow-y: scroll;
  right: 0;
  height: 100%;
  backdrop-filter: blur(5px);
`

const MarkdownValue = styled.div`
  display: block;
  background-color: color(display-p3 0 0 0 / 0.024);
  padding: 0.5em;
  overflow: hidden;
  line-height: initial;
  border-radius: 6px;
`

const SelectedNodeInfo: React.FC<{ graphData: IGraphData, selectedNode: CustomNode }> = (
  {
    graphData,
    selectedNode,
  }
) => {
  const fullCodeSourceLink = getFullRepoLink(graphData.attributes.repo_link, selectedNode.data.code_source)

  return (
    <Box mt="7">
      <Heading size="4" color="gray" weight="regular">Описание узла</Heading>

      <DataList.Root mt="5" orientation="vertical">
        <DataList.Item>
          <DataList.Label>ID</DataList.Label>
          <DataList.Value>
            <CopyArea>{selectedNode.id}</CopyArea>
          </DataList.Value>
        </DataList.Item>
        <DataList.Item>
          <DataList.Label>Тип узла</DataList.Label>
          <DataList.Value>{NODE_TYPE_ATTRIBUTES[selectedNode.type!].verbose_name}</DataList.Value>
        </DataList.Item>
        <DataList.Item>
          <DataList.Label>Техническое имя</DataList.Label>
          <DataList.Value><CopyArea>{selectedNode.data?.name || '-'}</CopyArea></DataList.Value>
        </DataList.Item>
        <DataList.Item>
          <DataList.Label>Название</DataList.Label>
          <DataList.Value><CopyArea>{selectedNode.data?.verbose_name || '-'}</CopyArea></DataList.Value>
        </DataList.Item>
        {
          selectedNode.data?.doc &&
            <DataList.Item>
                <DataList.Label>Документация</DataList.Label>
                <MarkdownValue>
                    <Markdown>{selectedNode.data.doc}</Markdown>
                </MarkdownValue>
            </DataList.Item>
        }
        {
          graphData.attributes.repo_link && selectedNode.data?.code_source &&
            <DataList.Item>
                <DataList.Label>Ссылка на репозиторий</DataList.Label>
                <DataList.Value>
                    <CopyArea>
                        <Link color="blue" target="_blank" href={fullCodeSourceLink}>{fullCodeSourceLink}</Link>
                    </CopyArea>
                </DataList.Value>
            </DataList.Item>
        }
      </DataList.Root>
    </Box>
  )
}

export const DetailsDrawer: React.FC = () => {
  const graphData = useStore((state) => state.graphData!)
  const selectedNode = useSelectedNode()

  return (
    <Wrapper>
      <Box p="4">
        <Heading size="4" color="gray" weight="regular">Информация о графе</Heading>

        <Box mt="5">
          <DataList.Root orientation="vertical">
            <DataList.Item>
              <DataList.Label>Техническое имя</DataList.Label>
              <DataList.Value><CopyArea>{graphData.attributes.name}</CopyArea></DataList.Value>
            </DataList.Item>
            <DataList.Item>
              <DataList.Label>Название</DataList.Label>
              <DataList.Value><CopyArea>{graphData.attributes.verbose_name}</CopyArea></DataList.Value>
            </DataList.Item>
            <DataList.Item>
              <DataList.Label>Количество узлов / рёбер</DataList.Label>
              <DataList.Value>{graphData.nodes.length} / {graphData.edges.length}</DataList.Value>
            </DataList.Item>
            <DataList.Item>
              <DataList.Label>Ссылка на репозиторий</DataList.Label>
              <DataList.Value>
                {
                  graphData.attributes.repo_link &&
                    <CopyArea>
                      <Link color="blue" target="_blank"
                            href={graphData.attributes.repo_link}>{graphData.attributes.repo_link}</Link>
                    </CopyArea>
                }
              </DataList.Value>
            </DataList.Item>
          </DataList.Root>
        </Box>

        {!selectedNode &&
            <Box mt="7">
                <Callout.Root>
                    <Callout.Icon>
                        <InfoCircledIcon/>
                    </Callout.Icon>
                    <Callout.Text>
                        Выберите узел, чтобы посмотреть его описание
                    </Callout.Text>
                </Callout.Root>
            </Box>
        }

        {selectedNode &&
            <SelectedNodeInfo graphData={graphData} selectedNode={selectedNode}/>
        }
      </Box>
    </Wrapper>
  )
}
