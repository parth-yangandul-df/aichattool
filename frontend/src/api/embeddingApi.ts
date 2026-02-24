import { api } from './client';

export interface EmbeddingTaskStatus {
  connection_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  total: number;
  completed: number;
  error: string | null;
}

export interface EmbeddingStatusResponse {
  tasks: EmbeddingTaskStatus[];
}

export const embeddingApi = {
  status: () =>
    api.get<EmbeddingStatusResponse>('/embeddings/status').then((r) => r.data),
};
