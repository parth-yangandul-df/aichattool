import { useState } from 'react';
import {
  Stack,
  Title,
  Button,
  Group,
  Text,
  Table,
  Modal,
  TextInput,
  Textarea,
  Select,
  NumberInput,
  ActionIcon,
  Tooltip,
  Alert,
  Loader,
  Badge,
} from '@mantine/core';
import { useForm } from '@mantine/form';
import { notifications } from '@mantine/notifications';
import { IconPlus, IconTrash } from '@tabler/icons-react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { dictionaryApi } from '../api/glossaryApi';
import { connectionApi } from '../api/connectionApi';
import { useConnections } from '../hooks/useConnections';
import type { DictionaryEntry, TableSummary, Column } from '../types/api';

export function DictionaryPage() {
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [tableId, setTableId] = useState<string | null>(null);
  const [columnId, setColumnId] = useState<string | null>(null);
  const [addOpen, setAddOpen] = useState(false);

  const { data: connections } = useConnections();
  const qc = useQueryClient();

  const connOptions =
    connections?.map((c) => ({ value: c.id, label: c.name })) ?? [];
  if (!connectionId && connOptions.length > 0) {
    setConnectionId(connOptions[0].value);
  }

  const { data: tables } = useQuery({
    queryKey: ['tables', connectionId],
    queryFn: () => connectionApi.tables(connectionId!),
    enabled: !!connectionId,
  });

  const { data: tableDetail } = useQuery({
    queryKey: ['tables', 'detail', tableId],
    queryFn: () => connectionApi.tableDetail(tableId!),
    enabled: !!tableId,
  });

  const tableOptions =
    tables?.map((t: TableSummary) => ({
      value: t.id,
      label: `${t.schema_name}.${t.table_name}`,
    })) ?? [];

  const columnOptions =
    tableDetail?.columns?.map((c: Column) => ({
      value: c.id,
      label: `${c.column_name} (${c.data_type})`,
    })) ?? [];

  const { data: entries, isLoading: entriesLoading } = useQuery({
    queryKey: ['dictionary', columnId],
    queryFn: () => dictionaryApi.list(columnId!),
    enabled: !!columnId,
  });

  const deleteMutation = useMutation({
    mutationFn: (entryId: string) => dictionaryApi.delete(columnId!, entryId),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ['dictionary', columnId] }),
  });

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Title order={2}>Data Dictionary</Title>
        <Button
          leftSection={<IconPlus size={16} />}
          onClick={() => setAddOpen(true)}
          disabled={!columnId}
        >
          Add Entry
        </Button>
      </Group>

      <Text size="sm" c="dimmed">
        Map raw column values to human-readable labels so the LLM understands
        coded values.
      </Text>

      <Group>
        <Select
          label="Connection"
          data={connOptions}
          value={connectionId}
          onChange={(v) => {
            setConnectionId(v);
            setTableId(null);
            setColumnId(null);
          }}
          w={250}
        />
        <Select
          label="Table"
          data={tableOptions}
          value={tableId}
          onChange={(v) => {
            setTableId(v);
            setColumnId(null);
          }}
          disabled={!connectionId}
          searchable
          w={300}
        />
        <Select
          label="Column"
          data={columnOptions}
          value={columnId}
          onChange={setColumnId}
          disabled={!tableId}
          searchable
          w={250}
        />
      </Group>

      {!columnId && (
        <Alert color="blue">
          Select a connection, table, and column to manage dictionary entries.
        </Alert>
      )}

      {entriesLoading && (
        <Group justify="center" py="xl">
          <Loader />
        </Group>
      )}

      {columnId && entries?.length === 0 && (
        <Alert color="blue">
          No dictionary entries for this column yet.
        </Alert>
      )}

      {entries && entries.length > 0 && (
        <Table striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Raw Value</Table.Th>
              <Table.Th>Display Value</Table.Th>
              <Table.Th>Description</Table.Th>
              <Table.Th w={60}>Actions</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {entries.map((e: DictionaryEntry) => (
              <Table.Tr key={e.id}>
                <Table.Td>
                  <Badge variant="outline">{e.raw_value}</Badge>
                </Table.Td>
                <Table.Td>{e.display_value}</Table.Td>
                <Table.Td>
                  <Text size="sm" c="dimmed">
                    {e.description}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Tooltip label="Delete">
                    <ActionIcon
                      variant="subtle"
                      color="red"
                      size="sm"
                      onClick={() => {
                        if (confirm('Delete this entry?'))
                          deleteMutation.mutate(e.id);
                      }}
                    >
                      <IconTrash size={14} />
                    </ActionIcon>
                  </Tooltip>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}

      {columnId && (
        <DictionaryFormModal
          opened={addOpen}
          onClose={() => setAddOpen(false)}
          columnId={columnId}
        />
      )}
    </Stack>
  );
}

function DictionaryFormModal({
  opened,
  onClose,
  columnId,
}: {
  opened: boolean;
  onClose: () => void;
  columnId: string;
}) {
  const qc = useQueryClient();

  const form = useForm({
    initialValues: {
      raw_value: '',
      display_value: '',
      description: '',
      sort_order: 0,
    },
    validate: {
      raw_value: (v) => (v.trim() ? null : 'Raw value is required'),
      display_value: (v) => (v.trim() ? null : 'Display value is required'),
    },
  });

  const mutation = useMutation({
    mutationFn: (values: Record<string, unknown>) =>
      dictionaryApi.create(columnId, values),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['dictionary', columnId] });
      notifications.show({
        title: 'Entry created',
        message: `Mapping "${form.values.raw_value}" → "${form.values.display_value}" added`,
        color: 'green',
      });
      form.reset();
      onClose();
    },
  });

  return (
    <Modal opened={opened} onClose={onClose} title="Add Dictionary Entry">
      <form onSubmit={form.onSubmit((v) => mutation.mutate(v))}>
        <Stack gap="sm">
          <TextInput
            label="Raw value"
            placeholder="e.g. A"
            required
            {...form.getInputProps('raw_value')}
          />
          <TextInput
            label="Display value"
            placeholder="e.g. Enterprise"
            required
            {...form.getInputProps('display_value')}
          />
          <Textarea
            label="Description"
            placeholder="Optional description"
            {...form.getInputProps('description')}
          />
          <NumberInput
            label="Sort order"
            {...form.getInputProps('sort_order')}
          />
          <Group justify="flex-end">
            <Button variant="subtle" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit" loading={mutation.isPending}>
              Create
            </Button>
          </Group>
        </Stack>
      </form>
    </Modal>
  );
}
