import { api } from './client';
import type { Connection, ConnectionCreate, IntrospectionResult, TableSummary, TableDetail } from '../types/api';

export const connectionApi = {
  list: () => api.get<Connection[]>('/connections').then(r => r.data),
  get: (id: string) => api.get<Connection>(`/connections/${id}`).then(r => r.data),
  create: (data: ConnectionCreate) => api.post<Connection>('/connections', data).then(r => r.data),
  update: (id: string, data: Partial<ConnectionCreate>) => api.put<Connection>(`/connections/${id}`, data).then(r => r.data),
  delete: (id: string) => api.delete(`/connections/${id}`),
  test: (id: string) => api.post<{ success: boolean; message: string | null }>(`/connections/${id}/test`).then(r => r.data),
  introspect: (id: string) => api.post<IntrospectionResult>(`/connections/${id}/introspect`).then(r => r.data),
  tables: (id: string) => api.get<TableSummary[]>(`/connections/${id}/tables`).then(r => r.data),
  tableDetail: (tableId: string) => api.get<TableDetail>(`/tables/${tableId}`).then(r => r.data),
};
