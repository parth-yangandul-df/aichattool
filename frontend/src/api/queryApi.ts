import { api } from './client';
import type { QueryResult, QueryHistory } from '../types/api';

export const queryApi = {
  execute: (data: { connection_id: string; question: string }) =>
    api.post<QueryResult>('/query', data).then(r => r.data),

  sqlOnly: (data: { connection_id: string; question: string }) =>
    api.post<{ generated_sql: string; explanation: string; confidence: number; tables_used: string[]; assumptions: string[] }>('/query/sql-only', data).then(r => r.data),

  executeSql: (data: { connection_id: string; sql: string; original_question?: string }) =>
    api.post<QueryResult>('/query/execute-sql', data).then(r => r.data),

  history: (params?: { connection_id?: string; limit?: number; offset?: number }) =>
    api.get<QueryHistory[]>('/query-history', { params }).then(r => r.data),

  toggleFavorite: (id: string) =>
    api.patch<{ is_favorite: boolean }>(`/query-history/${id}/favorite`).then(r => r.data),
};
