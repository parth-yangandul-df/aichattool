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
  Textarea,
  NumberInput,
  Select,
  ActionIcon,
  Tooltip,
  Alert,
  Loader,
  Accordion,
  ScrollArea,
  Divider,
  Box,
  SimpleGrid,
  ThemeIcon,
  Center,
} from '@mantine/core';
import { useForm } from '@mantine/form';
import { notifications } from '@mantine/notifications';
import {
  IconPlus,
  IconTrash,
  IconPlugConnected,
  IconRefresh,
  IconTable,
  IconChevronRight,
  IconChevronLeft,
  IconAlertCircle,
} from '@tabler/icons-react';
import {
  useConnections,
  useCreateConnection,
  useDeleteConnection,
  useTestConnection,
  useIntrospect,
  useTables,
  useAvailableTables,
  useUpdateConnection,
} from '../hooks/useConnections';
import type { Connection, ConnectionCreate, TableSummary, AvailableTable } from '../types/api';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function tableKey(schema: string, table: string) {
  return `${schema}.${table}`;
}

// ---------------------------------------------------------------------------
// Table Manager Modal (SQL Server only)
// ---------------------------------------------------------------------------

interface TableManagerModalProps {
  connection: Connection;
  opened: boolean;
  onClose: () => void;
}

function TableManagerModal({ connection, opened, onClose }: TableManagerModalProps) {
  const updateMutation = useUpdateConnection(connection.id);

  // Fetch all available dbo tables from the live DB (only when modal is open)
  const { data: availableTables, isLoading, isError } = useAvailableTables(
    connection.id,
    opened,
  );

  // Local whitelist state — seeded from connection on open
  const [whitelist, setWhitelist] = useState<string[]>(
    connection.allowed_table_names ?? [],
  );

  // Search filters for each column
  const [availableSearch, setAvailableSearch] = useState('');
  const [whitelistSearch, setWhitelistSearch] = useState('');

  // The full candidate set (available but not yet whitelisted)
  const whitelistSet = new Set(whitelist.map(n => n.toLowerCase()));
  const candidates: AvailableTable[] = (availableTables ?? []).filter(
    t => !whitelistSet.has(tableKey(t.schema_name, t.table_name).toLowerCase()),
  );

  // Apply search filters
  const filteredCandidates = candidates.filter(t =>
    t.table_name.toLowerCase().includes(availableSearch.toLowerCase()),
  );
  const filteredWhitelist = whitelist.filter(name =>
    name.toLowerCase().includes(whitelistSearch.toLowerCase()),
  );

  const addTable = (t: AvailableTable) => {
    const key = tableKey(t.schema_name, t.table_name);
    setWhitelist(prev => [...prev, key].sort());
  };

  const removeTable = (key: string) => {
    setWhitelist(prev => prev.filter(k => k !== key));
  };

  const addAll = () => {
    const allKeys = filteredCandidates.map(t => tableKey(t.schema_name, t.table_name));
    setWhitelist(prev => [...new Set([...prev, ...allKeys])].sort());
  };

  const removeAll = () => {
    // Remove only what's visible in the filtered whitelist
    const toRemove = new Set(filteredWhitelist.map(k => k.toLowerCase()));
    setWhitelist(prev => prev.filter(k => !toRemove.has(k.toLowerCase())));
  };

  const handleSave = () => {
    // Pass empty list to clear whitelist (backend converts [] → null)
    updateMutation.mutate(
      { allowed_table_names: whitelist.length > 0 ? whitelist : [] },
      {
        onSuccess: () => {
          notifications.show({
            title: 'Table whitelist saved',
            message: whitelist.length > 0
              ? `${whitelist.length} table${whitelist.length !== 1 ? 's' : ''} whitelisted. Re-run introspection to apply.`
              : 'Whitelist cleared — all dbo tables will be included on next introspection.',
            color: 'green',
          });
          onClose();
        },
        onError: () =>
          notifications.show({
            title: 'Save failed',
            message: 'Could not update table whitelist.',
            color: 'red',
          }),
      },
    );
  };

  const columnHeight = 380;

  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title={
        <Group gap="xs">
          <ThemeIcon size="sm" variant="light" color="blue">
            <IconTable size={14} />
          </ThemeIcon>
          <Text fw={600}>Manage Tables — {connection.name}</Text>
        </Group>
      }
      size="xl"
    >
      <Stack gap="sm">
        <Alert color="blue" variant="light" icon={<IconAlertCircle size={16} />}>
          Only <strong>dbo</strong> tables are shown. <code>TS_*</code> and backup tables are
          automatically excluded. Save changes, then re-run <strong>introspection</strong> to
          apply the filter.
        </Alert>

        {isError && (
          <Alert color="red" variant="light" icon={<IconAlertCircle size={16} />}>
            Failed to load available tables from the database. Check that the connection is active.
          </Alert>
        )}

        <SimpleGrid cols={2} spacing="md">
          {/* ── Left: Available (not yet whitelisted) ── */}
          <Stack gap="xs">
            <Group justify="space-between">
              <Text size="sm" fw={600} c="dimmed">
                Available ({filteredCandidates.length})
              </Text>
              <Button
                size="compact-xs"
                variant="subtle"
                disabled={filteredCandidates.length === 0}
                onClick={addAll}
              >
                Add all
              </Button>
            </Group>
            <TextInput
              placeholder="Search tables..."
              size="xs"
              value={availableSearch}
              onChange={e => setAvailableSearch(e.currentTarget.value)}
            />
            <Box
              style={{
                border: '1px solid var(--mantine-color-default-border)',
                borderRadius: 'var(--mantine-radius-sm)',
                height: columnHeight,
              }}
            >
              {isLoading ? (
                <Center h={columnHeight}>
                  <Loader size="sm" />
                </Center>
              ) : filteredCandidates.length === 0 ? (
                <Center h={columnHeight}>
                  <Text size="xs" c="dimmed">
                    {candidates.length === 0 ? 'All tables whitelisted' : 'No matches'}
                  </Text>
                </Center>
              ) : (
                <ScrollArea h={columnHeight} p="xs">
                  <Stack gap={2}>
                    {filteredCandidates.map(t => {
                      const key = tableKey(t.schema_name, t.table_name);
                      return (
                        <Group key={key} justify="space-between" wrap="nowrap">
                          <Text size="xs" style={{ fontFamily: 'monospace' }} truncate>
                            {key}
                          </Text>
                          <ActionIcon
                            size="xs"
                            variant="subtle"
                            color="blue"
                            onClick={() => addTable(t)}
                            title="Add to whitelist"
                          >
                            <IconChevronRight size={12} />
                          </ActionIcon>
                        </Group>
                      );
                    })}
                  </Stack>
                </ScrollArea>
              )}
            </Box>
          </Stack>

          {/* ── Right: Whitelisted ── */}
          <Stack gap="xs">
            <Group justify="space-between">
              <Text size="sm" fw={600} c="dimmed">
                Whitelisted ({filteredWhitelist.length}
                {whitelistSearch ? ` of ${whitelist.length}` : ''})
              </Text>
              <Button
                size="compact-xs"
                variant="subtle"
                color="red"
                disabled={filteredWhitelist.length === 0}
                onClick={removeAll}
              >
                Remove all
              </Button>
            </Group>
            <TextInput
              placeholder="Search whitelist..."
              size="xs"
              value={whitelistSearch}
              onChange={e => setWhitelistSearch(e.currentTarget.value)}
            />
            <Box
              style={{
                border: '1px solid var(--mantine-color-default-border)',
                borderRadius: 'var(--mantine-radius-sm)',
                height: columnHeight,
              }}
            >
              {whitelist.length === 0 ? (
                <Center h={columnHeight}>
                  <Stack align="center" gap={4}>
                    <Text size="xs" c="dimmed">No tables whitelisted</Text>
                    <Text size="xs" c="dimmed">All dbo tables will be included</Text>
                  </Stack>
                </Center>
              ) : filteredWhitelist.length === 0 ? (
                <Center h={columnHeight}>
                  <Text size="xs" c="dimmed">No matches</Text>
                </Center>
              ) : (
                <ScrollArea h={columnHeight} p="xs">
                  <Stack gap={2}>
                    {filteredWhitelist.map(key => (
                      <Group key={key} justify="space-between" wrap="nowrap">
                        <ActionIcon
                          size="xs"
                          variant="subtle"
                          color="red"
                          onClick={() => removeTable(key)}
                          title="Remove from whitelist"
                        >
                          <IconChevronLeft size={12} />
                        </ActionIcon>
                        <Text
                          size="xs"
                          style={{ fontFamily: 'monospace', flex: 1 }}
                          truncate
                        >
                          {key}
                        </Text>
                      </Group>
                    ))}
                  </Stack>
                </ScrollArea>
              )}
            </Box>
          </Stack>
        </SimpleGrid>

        <Divider />

        <Group justify="space-between">
          <Text size="xs" c="dimmed">
            {whitelist.length === 0
              ? 'No whitelist — all dbo tables (minus auto-excluded) will be cached on introspection.'
              : `${whitelist.length} table${whitelist.length !== 1 ? 's' : ''} whitelisted — only these will be cached on introspection.`}
          </Text>
          <Group gap="xs">
            <Button variant="subtle" onClick={onClose}>
              Cancel
            </Button>
            <Button onClick={handleSave} loading={updateMutation.isPending}>
              Save Changes
            </Button>
          </Group>
        </Group>
      </Stack>
    </Modal>
  );
}

// ---------------------------------------------------------------------------
// Schema Explorer (existing, unchanged)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Connection Form types & modal (existing, with minor additions)
// ---------------------------------------------------------------------------

interface ConnectionFormValues {
  name: string;
  connector_type: string;
  // PostgreSQL / SQL Server fields
  connection_string: string;
  // BigQuery fields
  bq_project_id: string;
  bq_credentials_json: string;
  // Databricks fields
  db_server_hostname: string;
  db_http_path: string;
  db_access_token: string;
  db_catalog: string;
  // Shared
  default_schema: string;
  max_query_timeout_seconds: number;
  max_rows: number;
}

function AddConnectionModal({
  opened,
  onClose,
}: {
  opened: boolean;
  onClose: () => void;
}) {
  const createMutation = useCreateConnection();

  const form = useForm<ConnectionFormValues>({
    initialValues: {
      name: '',
      connector_type: 'postgresql',
      connection_string: '',
      bq_project_id: '',
      bq_credentials_json: '',
      db_server_hostname: '',
      db_http_path: '',
      db_access_token: '',
      db_catalog: 'main',
      default_schema: 'public',
      max_query_timeout_seconds: 30,
      max_rows: 1000,
    },
    validate: {
      name: (v) => (v.trim() ? null : 'Name is required'),
      connection_string: (v, values) =>
        ['postgresql', 'sqlserver'].includes(values.connector_type) && !v.trim()
          ? 'Connection string is required'
          : null,
      bq_project_id: (v, values) =>
        values.connector_type === 'bigquery' && !v.trim()
          ? 'Project ID is required'
          : null,
      bq_credentials_json: (v, values) => {
        if (values.connector_type !== 'bigquery') return null;
        if (!v.trim()) return 'Service account JSON is required';
        try {
          JSON.parse(v);
          return null;
        } catch {
          return 'Invalid JSON';
        }
      },
      db_server_hostname: (v, values) =>
        values.connector_type === 'databricks' && !v.trim()
          ? 'Server hostname is required'
          : null,
      db_http_path: (v, values) =>
        values.connector_type === 'databricks' && !v.trim()
          ? 'HTTP path is required'
          : null,
      db_access_token: (v, values) =>
        values.connector_type === 'databricks' && !v.trim()
          ? 'Access token is required'
          : null,
    },
  });

  const connectorType = form.values.connector_type;
  const isBigQuery = connectorType === 'bigquery';
  const isDatabricks = connectorType === 'databricks';
  const isSqlServer = connectorType === 'sqlserver';

  const handleSubmit = (values: ConnectionFormValues) => {
    let connectionString = values.connection_string;
    let defaultSchema = values.default_schema;

    if (values.connector_type === 'bigquery') {
      connectionString = JSON.stringify({
        project_id: values.bq_project_id,
        credentials_json: JSON.parse(values.bq_credentials_json),
      });
      if (!defaultSchema || defaultSchema === 'public') {
        defaultSchema = '';
      }
    } else if (values.connector_type === 'databricks') {
      connectionString = JSON.stringify({
        server_hostname: values.db_server_hostname,
        http_path: values.db_http_path,
        access_token: values.db_access_token,
        catalog: values.db_catalog || 'main',
      });
      if (!defaultSchema || defaultSchema === 'public') {
        defaultSchema = 'default';
      }
    } else if (values.connector_type === 'sqlserver') {
      if (!defaultSchema || defaultSchema === 'public') {
        defaultSchema = 'dbo';
      }
    }

    const payload: ConnectionCreate = {
      name: values.name,
      connector_type: values.connector_type,
      connection_string: connectionString,
      default_schema: defaultSchema,
      max_query_timeout_seconds: values.max_query_timeout_seconds,
      max_rows: values.max_rows,
    };

    createMutation.mutate(payload, {
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
            data={[
              { value: 'postgresql', label: 'PostgreSQL' },
              { value: 'sqlserver', label: 'SQL Server' },
              { value: 'bigquery', label: 'BigQuery' },
              { value: 'databricks', label: 'Databricks' },
            ]}
            {...form.getInputProps('connector_type')}
          />

          {isBigQuery ? (
            <>
              <TextInput
                label="Project ID"
                placeholder="my-gcp-project"
                required
                {...form.getInputProps('bq_project_id')}
              />
              <Textarea
                label="Service account JSON"
                placeholder="Paste the contents of your service account key file"
                required
                autosize
                minRows={4}
                maxRows={10}
                styles={{ input: { fontFamily: 'monospace', fontSize: 12 } }}
                {...form.getInputProps('bq_credentials_json')}
              />
            </>
          ) : isDatabricks ? (
            <>
              <TextInput
                label="Server hostname"
                placeholder="dbc-a1b2345c-d6e7.cloud.databricks.com"
                required
                {...form.getInputProps('db_server_hostname')}
              />
              <TextInput
                label="HTTP path"
                placeholder="/sql/1.0/warehouses/a1b234c567d8e9fa"
                required
                {...form.getInputProps('db_http_path')}
              />
              <TextInput
                label="Access token"
                placeholder="dapi..."
                type="password"
                required
                {...form.getInputProps('db_access_token')}
              />
              <TextInput
                label="Catalog"
                placeholder="main"
                {...form.getInputProps('db_catalog')}
              />
            </>
          ) : (
            <TextInput
              label="Connection string"
              placeholder={
                isSqlServer
                  ? 'SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=your-password;Encrypt=yes;TrustServerCertificate=yes;'
                  : 'postgresql://user:pass@host:5432/dbname'
              }
              required
              {...form.getInputProps('connection_string')}
            />
          )}

          <TextInput
            label={isBigQuery ? 'Dataset' : 'Default schema'}
            placeholder={
              isBigQuery ? 'my_dataset' : isDatabricks || isSqlServer ? 'dbo' : 'public'
            }
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
          {isSqlServer && (
            <Alert color="blue" variant="light" icon={<IconAlertCircle size={16} />}>
              Only <strong>dbo</strong> schema tables will be introspected. <code>TS_*</code> and
              backup tables are excluded automatically. Use <strong>Manage Tables</strong> after
              creation to set a specific table whitelist.
            </Alert>
          )}
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

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export function ConnectionsPage() {
  const [addOpen, setAddOpen] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [tableManagerConn, setTableManagerConn] = useState<Connection | null>(null);

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

      {connections?.map((conn: Connection) => (
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
              {/* SQL Server whitelist status badge */}
              {conn.connector_type === 'sqlserver' && (
                <Tooltip
                  label={
                    conn.allowed_table_names && conn.allowed_table_names.length > 0
                      ? `Whitelist: ${conn.allowed_table_names.join(', ')}`
                      : 'All dbo tables included (no whitelist set)'
                  }
                  multiline
                  maw={320}
                >
                  <Badge
                    size="sm"
                    color={
                      conn.allowed_table_names && conn.allowed_table_names.length > 0
                        ? 'violet'
                        : 'gray'
                    }
                    variant="light"
                    style={{ cursor: 'default' }}
                  >
                    {conn.allowed_table_names && conn.allowed_table_names.length > 0
                      ? `${conn.allowed_table_names.length} tables`
                      : 'dbo only'}
                  </Badge>
                </Tooltip>
              )}
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
              {/* Manage Tables button — SQL Server only */}
              {conn.connector_type === 'sqlserver' && (
                <Tooltip label="Manage table whitelist">
                  <ActionIcon
                    variant="subtle"
                    color="violet"
                    onClick={() => setTableManagerConn(conn)}
                  >
                    <IconTable size={18} />
                  </ActionIcon>
                </Tooltip>
              )}
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

      {tableManagerConn && (
        <TableManagerModal
          connection={tableManagerConn}
          opened={!!tableManagerConn}
          onClose={() => setTableManagerConn(null)}
        />
      )}
    </Stack>
  );
}
