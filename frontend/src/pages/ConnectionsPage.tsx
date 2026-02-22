import { useState } from 'react';
import {
  Stack,
  Title,
  Button,
  Group,
  Paper,
  Text,
  Badge,
  Modal,
  TextInput,
  NumberInput,
  Select,
  ActionIcon,
  Tooltip,
  Alert,
  Loader,
  Accordion,
} from '@mantine/core';
import { useForm } from '@mantine/form';
import { notifications } from '@mantine/notifications';
import {
  IconPlus,
  IconTrash,
  IconPlugConnected,
  IconRefresh,
} from '@tabler/icons-react';
import {
  useConnections,
  useCreateConnection,
  useDeleteConnection,
  useTestConnection,
  useIntrospect,
  useTables,
} from '../hooks/useConnections';
import type { ConnectionCreate, TableSummary } from '../types/api';

export function ConnectionsPage() {
  const [addOpen, setAddOpen] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const { data: connections, isLoading } = useConnections();
  const deleteMutation = useDeleteConnection();
  const testMutation = useTestConnection();
  const introspectMutation = useIntrospect();

  const handleTest = (id: string) => {
    testMutation.mutate(id, {
      onSuccess: (res) =>
        notifications.show({
          title: res.success ? 'Connected' : 'Failed',
          message: res.success
            ? 'Connection successful'
            : res.message || 'Connection failed',
          color: res.success ? 'green' : 'red',
        }),
    });
  };

  const handleIntrospect = (id: string) => {
    introspectMutation.mutate(id, {
      onSuccess: (res) =>
        notifications.show({
          title: 'Introspection complete',
          message: `Found ${res.tables_found} tables, ${res.columns_found} columns, ${res.relationships_found} relationships`,
          color: 'green',
        }),
    });
  };

  const handleDelete = (id: string) => {
    if (confirm('Delete this connection?')) {
      deleteMutation.mutate(id);
    }
  };

  if (isLoading)
    return (
      <Group justify="center" py="xl">
        <Loader />
      </Group>
    );

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Title order={2}>Database Connections</Title>
        <Button leftSection={<IconPlus size={16} />} onClick={() => setAddOpen(true)}>
          Add Connection
        </Button>
      </Group>

      {connections?.length === 0 && (
        <Alert color="blue">
          No connections yet. Add one to get started.
        </Alert>
      )}

      {connections?.map((conn) => (
        <Paper key={conn.id} withBorder p="md">
          <Group justify="space-between" mb="xs">
            <Group>
              <Text fw={600}>{conn.name}</Text>
              <Badge size="sm" variant="light">
                {conn.connector_type}
              </Badge>
              <Badge
                size="sm"
                color={conn.is_active ? 'green' : 'gray'}
                variant="light"
              >
                {conn.is_active ? 'active' : 'inactive'}
              </Badge>
            </Group>
            <Group gap="xs">
              <Tooltip label="Test connection">
                <ActionIcon
                  variant="subtle"
                  onClick={() => handleTest(conn.id)}
                  loading={
                    testMutation.isPending &&
                    testMutation.variables === conn.id
                  }
                >
                  <IconPlugConnected size={18} />
                </ActionIcon>
              </Tooltip>
              <Tooltip label="Introspect schema">
                <ActionIcon
                  variant="subtle"
                  color="blue"
                  onClick={() => handleIntrospect(conn.id)}
                  loading={
                    introspectMutation.isPending &&
                    introspectMutation.variables === conn.id
                  }
                >
                  <IconRefresh size={18} />
                </ActionIcon>
              </Tooltip>
              <Tooltip label="Delete">
                <ActionIcon
                  variant="subtle"
                  color="red"
                  onClick={() => handleDelete(conn.id)}
                >
                  <IconTrash size={18} />
                </ActionIcon>
              </Tooltip>
            </Group>
          </Group>
          <Text size="sm" c="dimmed">
            Schema: {conn.default_schema} | Timeout:{' '}
            {conn.max_query_timeout_seconds}s | Max rows: {conn.max_rows}
          </Text>
          {conn.last_introspected_at && (
            <Text size="xs" c="dimmed" mt={4}>
              Last introspected:{' '}
              {new Date(conn.last_introspected_at).toLocaleString()}
            </Text>
          )}
          <Button
            variant="subtle"
            size="xs"
            mt="xs"
            onClick={() =>
              setSelectedId(selectedId === conn.id ? null : conn.id)
            }
          >
            {selectedId === conn.id ? 'Hide tables' : 'Show tables'}
          </Button>
          {selectedId === conn.id && <SchemaExplorer connectionId={conn.id} />}
        </Paper>
      ))}

      <AddConnectionModal
        opened={addOpen}
        onClose={() => setAddOpen(false)}
      />
    </Stack>
  );
}

function SchemaExplorer({ connectionId }: { connectionId: string }) {
  const { data: tables, isLoading } = useTables(connectionId);

  if (isLoading)
    return (
      <Group justify="center" py="sm">
        <Loader size="sm" />
      </Group>
    );

  if (!tables || tables.length === 0)
    return (
      <Text size="sm" c="dimmed" mt="xs">
        No tables found. Run introspection first.
      </Text>
    );

  return (
    <Accordion variant="separated" mt="sm">
      {tables.map((t: TableSummary) => (
        <Accordion.Item key={t.id} value={t.id}>
          <Accordion.Control>
            <Group>
              <Text size="sm" fw={500}>
                {t.schema_name}.{t.table_name}
              </Text>
              <Badge size="xs" variant="light">
                {t.column_count} cols
              </Badge>
              {t.row_count_estimate != null && (
                <Badge size="xs" variant="light" color="gray">
                  ~{t.row_count_estimate.toLocaleString()} rows
                </Badge>
              )}
            </Group>
          </Accordion.Control>
          <Accordion.Panel>
            {t.comment && (
              <Text size="sm" c="dimmed" mb="xs">
                {t.comment}
              </Text>
            )}
            <Text size="xs" c="dimmed">
              Type: {t.table_type}
            </Text>
          </Accordion.Panel>
        </Accordion.Item>
      ))}
    </Accordion>
  );
}

function AddConnectionModal({
  opened,
  onClose,
}: {
  opened: boolean;
  onClose: () => void;
}) {
  const createMutation = useCreateConnection();

  const form = useForm<ConnectionCreate>({
    initialValues: {
      name: '',
      connector_type: 'postgresql',
      connection_string: '',
      default_schema: 'public',
      max_query_timeout_seconds: 30,
      max_rows: 1000,
    },
    validate: {
      name: (v) => (v.trim() ? null : 'Name is required'),
      connection_string: (v) =>
        v.trim() ? null : 'Connection string is required',
    },
  });

  const handleSubmit = (values: ConnectionCreate) => {
    createMutation.mutate(values, {
      onSuccess: () => {
        notifications.show({
          title: 'Connection created',
          message: `"${values.name}" added successfully`,
          color: 'green',
        });
        form.reset();
        onClose();
      },
      onError: (err) =>
        notifications.show({
          title: 'Error',
          message: (err as Error).message,
          color: 'red',
        }),
    });
  };

  return (
    <Modal opened={opened} onClose={onClose} title="Add Database Connection" size="lg">
      <form onSubmit={form.onSubmit(handleSubmit)}>
        <Stack gap="sm">
          <TextInput
            label="Name"
            placeholder="My Production DB"
            required
            {...form.getInputProps('name')}
          />
          <Select
            label="Connector type"
            data={[{ value: 'postgresql', label: 'PostgreSQL' }]}
            {...form.getInputProps('connector_type')}
          />
          <TextInput
            label="Connection string"
            placeholder="postgresql://user:pass@host:5432/dbname"
            required
            {...form.getInputProps('connection_string')}
          />
          <TextInput
            label="Default schema"
            {...form.getInputProps('default_schema')}
          />
          <Group grow>
            <NumberInput
              label="Query timeout (seconds)"
              min={1}
              max={300}
              {...form.getInputProps('max_query_timeout_seconds')}
            />
            <NumberInput
              label="Max rows"
              min={1}
              max={100000}
              {...form.getInputProps('max_rows')}
            />
          </Group>
          <Group justify="flex-end">
            <Button variant="subtle" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit" loading={createMutation.isPending}>
              Create
            </Button>
          </Group>
        </Stack>
      </form>
    </Modal>
  );
}
