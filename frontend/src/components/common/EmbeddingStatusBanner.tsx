import { Alert, Progress, Text, Group } from '@mantine/core';
import { IconBrain } from '@tabler/icons-react';
import { useState } from 'react';
import { useEmbeddingStatus } from '../../hooks/useEmbeddingStatus';

export function EmbeddingStatusBanner() {
  const { data } = useEmbeddingStatus();
  const [dismissed, setDismissed] = useState<Set<string>>(new Set());

  if (!data) return null;

  const activeTasks = data.tasks.filter(
    (t) =>
      (t.status === 'running' || t.status === 'pending') &&
      !dismissed.has(t.connection_id),
  );

  if (activeTasks.length === 0) return null;

  return (
    <>
      {activeTasks.map((task) => {
        const pct =
          task.total > 0
            ? Math.round((task.completed / task.total) * 100)
            : 0;
        return (
          <Alert
            key={task.connection_id}
            icon={<IconBrain size={18} />}
            color="blue"
            variant="light"
            mb="sm"
            withCloseButton
            onClose={() =>
              setDismissed((prev) => new Set(prev).add(task.connection_id))
            }
          >
            <Group justify="space-between" mb={4}>
              <Text size="sm">
                Generating embeddings for semantic search...
              </Text>
              <Text size="sm" c="dimmed">
                {task.completed} / {task.total}
              </Text>
            </Group>
            <Progress value={pct} size="sm" animated />
          </Alert>
        );
      })}
    </>
  );
}
