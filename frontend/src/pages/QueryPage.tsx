import { useState } from 'react';
import {
  Stack,
  Title,
  Textarea,
  Button,
  Group,
  Select,
  Paper,
  Text,
  Table,
  Badge,
  Alert,
  Loader,
  Code,
  Accordion,
  CopyButton,
  ActionIcon,
  Tooltip,
  Progress,
} from '@mantine/core';
import { IconSend, IconCopy, IconCheck, IconAlertCircle, IconPlayerPlay, IconX, IconEdit } from '@tabler/icons-react';
import { useMutation } from '@tanstack/react-query';
import Editor from '@monaco-editor/react';
import { queryApi } from '../api/queryApi';
import { useConnections } from '../hooks/useConnections';
import type { QueryResult } from '../types/api';

interface SqlPreview {
  sql: string;
  explanation: string;
  confidence: number;
  tables_used: string[];
}

export function QueryPage() {
  const [question, setQuestion] = useState('');
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [sqlPreview, setSqlPreview] = useState<SqlPreview | null>(null);
  const [editedSql, setEditedSql] = useState('');

  const { data: connections, isLoading: loadingConns } = useConnections();

  const sqlOnlyMutation = useMutation({
    mutationFn: () =>
      queryApi.sqlOnly({ connection_id: connectionId!, question }),
    onSuccess: (data) => {
      setSqlPreview({
        sql: data.generated_sql,
        explanation: data.explanation,
        confidence: data.confidence,
        tables_used: data.tables_used,
      });
      setEditedSql(data.generated_sql);
    },
  });

  const executeMutation = useMutation({
    mutationFn: () =>
      queryApi.executeSql({
        connection_id: connectionId!,
        sql: editedSql,
        original_question: question,
      }),
    onSuccess: (data) => {
      setResult(data);
      setSqlPreview(null);
    },
  });

  const connOptions =
    connections?.map((c) => ({ value: c.id, label: c.name })) ?? [];

  // Auto-select first connection
  if (!connectionId && connOptions.length > 0) {
    setConnectionId(connOptions[0].value);
  }

  const handleRunQuery = () => {
    setResult(null);
    sqlOnlyMutation.mutate();
  };

  return (
    <Stack gap="md">
      <Title order={2}>Ask a Question</Title>

      <Group align="flex-end">
        <Select
          label="Database connection"
          placeholder="Select connection..."
          data={connOptions}
          value={connectionId}
          onChange={setConnectionId}
          disabled={loadingConns}
          w={300}
        />
      </Group>

      <Textarea
        placeholder="e.g. What is the total ECL by stage?"
        autosize
        minRows={2}
        maxRows={6}
        value={question}
        onChange={(e) => setQuestion(e.currentTarget.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
            e.preventDefault();
            if (connectionId && question.trim()) handleRunQuery();
          }
        }}
      />

      <Group>
        <Button
          leftSection={<IconSend size={16} />}
          onClick={handleRunQuery}
          loading={sqlOnlyMutation.isPending}
          disabled={!connectionId || !question.trim()}
        >
          Run Query
        </Button>
      </Group>

      {(sqlOnlyMutation.isError || executeMutation.isError) && (
        <Alert
          color="red"
          icon={<IconAlertCircle size={16} />}
          title="Query failed"
        >
          {((sqlOnlyMutation.error || executeMutation.error) as Error).message}
        </Alert>
      )}

      {executeMutation.isPending && (
        <Group justify="center" py="xl">
          <Loader size="lg" />
          <Text>Executing query...</Text>
        </Group>
      )}

      {sqlPreview && !executeMutation.isPending && (
        <Paper withBorder p="md">
          <Group justify="space-between" mb="xs">
            <Group gap="xs">
              <Text fw={600}>Review Generated SQL</Text>
              <Tooltip label="You can edit the SQL before executing">
                <IconEdit size={16} style={{ color: 'var(--mantine-color-dimmed)' }} />
              </Tooltip>
            </Group>
          </Group>
          {sqlPreview.explanation && (
            <Text size="sm" c="dimmed" mb="sm">
              {sqlPreview.explanation}
            </Text>
          )}
          <div style={{ border: '1px solid var(--mantine-color-dark-4)', borderRadius: '4px', overflow: 'hidden' }}>
            <Editor
              height="200px"
              defaultLanguage="sql"
              value={editedSql}
              onChange={(value) => setEditedSql(value ?? '')}
              theme="vs-dark"
              options={{
                minimap: { enabled: false },
                fontSize: 13,
                lineNumbers: 'on',
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                automaticLayout: true,
                padding: { top: 8, bottom: 8 },
              }}
            />
          </div>
          <Group mt="sm" gap="xl" align="flex-start">
            <Stack gap={4}>
              <Text size="xs" c="dimmed" fw={500}>CONFIDENCE</Text>
              <Group gap="xs" align="center">
                <Progress
                  value={sqlPreview.confidence * 100}
                  color={sqlPreview.confidence >= 0.8 ? 'green' : sqlPreview.confidence >= 0.5 ? 'yellow' : 'red'}
                  size="sm"
                  w={80}
                />
                <Text size="sm" fw={500}>{Math.round(sqlPreview.confidence * 100)}%</Text>
              </Group>
            </Stack>
            {sqlPreview.tables_used.length > 0 && (
              <Stack gap={4}>
                <Text size="xs" c="dimmed" fw={500}>TABLES USED</Text>
                <Group gap="xs">
                  {sqlPreview.tables_used.map((t) => (
                    <Badge key={t} variant="light" color="blue" size="sm">{t}</Badge>
                  ))}
                </Group>
              </Stack>
            )}
          </Group>
          <Group justify="flex-end" mt="md">
            <Button
              variant="default"
              leftSection={<IconX size={16} />}
              onClick={() => setSqlPreview(null)}
            >
              Cancel
            </Button>
            <Button
              color="green"
              leftSection={<IconPlayerPlay size={16} />}
              onClick={() => executeMutation.mutate()}
              disabled={!editedSql.trim()}
            >
              Execute
            </Button>
          </Group>
        </Paper>
      )}

      {result && <QueryResultView result={result} />}
    </Stack>
  );
}

function QueryResultView({ result }: { result: QueryResult }) {
  return (
    <Stack gap="md">
      {result.summary && (
        <Paper withBorder p="md" bg="blue.0">
          <Text fw={600} mb="xs">
            Summary
          </Text>
          <Text>{result.summary}</Text>
          {result.highlights.length > 0 && (
            <Group mt="xs" gap="xs">
              {result.highlights.map((h, i) => (
                <Badge key={i} variant="light">
                  {h}
                </Badge>
              ))}
            </Group>
          )}
        </Paper>
      )}

      <Accordion variant="contained">
        <Accordion.Item value="sql">
          <Accordion.Control>
            <Group>
              <Text fw={500}>SQL</Text>
              <Badge size="sm" variant="light">
                {result.execution_time_ms}ms
              </Badge>
              <Badge size="sm" variant="light" color="gray">
                {result.row_count} rows
              </Badge>
              {result.retry_count > 0 && (
                <Badge size="sm" color="yellow">
                  {result.retry_count} retries
                </Badge>
              )}
            </Group>
          </Accordion.Control>
          <Accordion.Panel>
            <Group justify="flex-end" mb="xs">
              <CopyButton value={result.final_sql}>
                {({ copied, copy }) => (
                  <Tooltip label={copied ? 'Copied' : 'Copy'}>
                    <ActionIcon variant="subtle" onClick={copy}>
                      {copied ? (
                        <IconCheck size={16} />
                      ) : (
                        <IconCopy size={16} />
                      )}
                    </ActionIcon>
                  </Tooltip>
                )}
              </CopyButton>
            </Group>
            <Code block>{result.final_sql}</Code>
          </Accordion.Panel>
        </Accordion.Item>
      </Accordion>

      {result.rows.length > 0 && (
        <Paper withBorder>
          <Table striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                {result.columns.map((col) => (
                  <Table.Th key={col}>{col}</Table.Th>
                ))}
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {result.rows.map((row, i) => (
                <Table.Tr key={i}>
                  {row.map((cell, j) => (
                    <Table.Td key={j}>
                      {cell === null ? (
                        <Text c="dimmed" fs="italic" size="sm">
                          null
                        </Text>
                      ) : (
                        String(cell)
                      )}
                    </Table.Td>
                  ))}
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
          {result.truncated && (
            <Text size="sm" c="dimmed" ta="center" py="xs">
              Results truncated to {result.row_count} rows
            </Text>
          )}
        </Paper>
      )}

      {result.suggested_followups.length > 0 && (
        <Paper withBorder p="md">
          <Text fw={600} mb="xs">
            Suggested follow-up questions
          </Text>
          <Stack gap="xs">
            {result.suggested_followups.map((q, i) => (
              <Text key={i} size="sm" c="blue">
                {q}
              </Text>
            ))}
          </Stack>
        </Paper>
      )}
    </Stack>
  );
}
