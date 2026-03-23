import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { connectionApi } from '../api/connectionApi';
import type { ConnectionCreate, Connection } from '../types/api';

export function useConnections() {
  return useQuery({
    queryKey: ['connections'],
    queryFn: connectionApi.list,
  });
}

export function useConnection(id: string | undefined) {
  return useQuery({
    queryKey: ['connections', id],
    queryFn: () => connectionApi.get(id!),
    enabled: !!id,
  });
}

export function useCreateConnection() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: ConnectionCreate) => connectionApi.create(data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['connections'] }),
  });
}

export function useUpdateConnection(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: Partial<ConnectionCreate>) =>
      connectionApi.update(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['connections'] });
      qc.invalidateQueries({ queryKey: ['connections', id] });
    },
  });
}

export function useDeleteConnection() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => connectionApi.delete(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['connections'] }),
  });
}

export function useTestConnection() {
  return useMutation({
    mutationFn: (id: string) => connectionApi.test(id),
  });
}

export function useIntrospect() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => connectionApi.introspect(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['tables'] }),
  });
}

export function useTables(connectionId: string | undefined) {
  return useQuery({
    queryKey: ['tables', connectionId],
    queryFn: () => connectionApi.tables(connectionId!),
    enabled: !!connectionId,
  });
}

export function useTableDetail(tableId: string | undefined) {
  return useQuery({
    queryKey: ['tables', 'detail', tableId],
    queryFn: () => connectionApi.tableDetail(tableId!),
    enabled: !!tableId,
  });
}

/** SQL Server only: fetches all dbo tables eligible for whitelisting (does not update cache). */
export function useAvailableTables(connectionId: string | undefined, enabled: boolean) {
  return useQuery({
    queryKey: ['available-tables', connectionId],
    queryFn: () => connectionApi.availableTables(connectionId!),
    enabled: !!connectionId && enabled,
    staleTime: 30_000, // 30s — these don't change often
  });
}

export function useActiveConnection() {
  // Simple local storage based active connection selection
  const stored = localStorage.getItem('activeConnectionId');
  const { data: connections } = useConnections();

  const activeId =
    stored && connections?.find((c: Connection) => c.id === stored)
      ? stored
      : connections?.[0]?.id ?? null;

  const setActive = (id: string) => {
    localStorage.setItem('activeConnectionId', id);
  };

  return { activeConnectionId: activeId, setActive };
}

