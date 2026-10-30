export const API_URL: string = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

export function currentUser(): string {
  try {
    return localStorage.getItem('pipeline.user') || 'alice';
  } catch {
    return 'alice';
  }
}

export class ApiError extends Error {
  constructor(public status: number, public body: unknown, public retryAfter?: number) {
    super(typeof body === 'object' && body && 'detail' in body ? String((body as { detail: unknown }).detail) : `HTTP ${status}`);
  }
}

async function request<T>(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<T> {
  const resp = await fetch(`${API_URL}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${currentUser()}`,
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
      ...headers,
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (resp.status === 204) return undefined as T;
  const data = await resp.json().catch(() => null);
  if (!resp.ok) {
    const ra = Number(resp.headers.get('Retry-After'));
    throw new ApiError(resp.status, data, Number.isFinite(ra) ? ra : undefined);
  }
  return data as T;
}

// --- types --------------------------------------------------------------------------------------------------

export interface FileSpec { name: string; size: number; mimeType: string; sha256: string }
export interface NotifySpec { channels: Array<'in_app' | 'email' | 'webhook'>; webhookUrl?: string; email?: string; notifyOn?: Array<'first_failure'> }

export interface PartUrl { partNumber: number; url: string }
export type UploadPlan =
  | { multipart: false; url: string; headers: Record<string, string>; expiresIn: number }
  | { multipart: true; uploadId: string; partSize: number; partCount: number; parts: PartUrl[]; expiresIn: number };

export interface PipelineError { kind: string; code: string; message: string; stage: string; retry_at?: string | null }

export interface CreatedTask {
  taskId: string; fileId: string; fileName: string; sha256: string; sizeBytes: number; status: string;
  error: PipelineError | null; upload?: UploadPlan | null; duplicateOf?: string;
}
export interface CreatedBatch { batchId: string; status: string; tasks: CreatedTask[] }

export interface StatusText { text: string; retryAt: string | null; queuePosition: number | null; etaS: number | null; startedAt: string | null }
export interface TaskSummary {
  taskId: string; fileId: string; fileName: string; fileType: string | null; sizeBytes: number; status: string;
  stage: 'upload' | 'extract' | 'deliver'; progress: number; deliveryStatus: string | null;
  attempts: { extract: number; deliver: number }; error: PipelineError | null; statusText: StatusText;
  actions: string[]; warnings: number; updatedAt: string;
}
export interface BatchView {
  batchId: string; status: string; progress: number; fileCount: number; counts: Record<string, number>;
  summary: string; createdAt: string; completedAt: string | null; notify: NotifySpec | null; lastEventId: number;
  tasks: TaskSummary[];
}
export interface StageInfo { status: string; finishedAt: string | null; attempts?: number; maxAttempts?: number; error?: PipelineError; vendorRef?: string | null; warnings?: number; nextAttemptAt?: string | null }
export interface TaskDetail extends TaskSummary {
  batchId: string; sha256: string; stages: Record<'upload' | 'extract' | 'deliver', StageInfo>; lastError: PipelineError | null;
}
export interface TaskEvent {
  id: number; taskId: string; batchId: string; at: string; stage: string; from: string | null; to: string;
  progress: number | null; attempt: number | null; errorKind: string | null; errorCode: string | null;
  message: string | null; nextRetryAt: string | null;
}
export interface BatchListItem { batchId: string; status: string; fileCount: number; counts: Record<string, number>; createdAt: string; completedAt: string | null }
export interface SystemStatus {
  vendor: { inflight: number; slots: number; maxConcurrent: number; pausedForS: number; throughputPerS: number;
    breaker: { state: 'closed' | 'open' | 'half_open'; openUntil: number | null } };
  queue: { waiting: number; delayed: number; waitingByUser: Record<string, number>; inflightByUser: Record<string, number> };
}

// --- endpoints ----------------------------------------------------------------------------------------------

export const api = {
  createBatch: (files: FileSpec[], opts: { idempotencyKey: string; notify?: NotifySpec }) =>
    request<CreatedBatch>('POST', '/batches', { files, notify: opts.notify }, { 'Idempotency-Key': opts.idempotencyKey }),
  listBatches: () => request<{ batches: BatchListItem[] }>('GET', '/batches'),
  getBatch: (id: string) => request<BatchView>('GET', `/batches/${id}?limit=500`),
  retryFailed: (id: string) => request<{ replayed: number }>('POST', `/batches/${id}/retry-failed`),
  setNotify: (id: string, notify: NotifySpec) => request('PUT', `/batches/${id}/notify`, notify),
  completeFile: (fileId: string, parts: Array<{ partNumber: number; etag: string }>) =>
    request<TaskSummary>('POST', `/files/${fileId}/complete`, { parts }),
  refreshUrls: (fileId: string, parts?: number[]) => request<UploadPlan>('POST', `/files/${fileId}/refresh-urls`, { parts }),
  getParts: (fileId: string) =>
    request<{ status: string; parts: Array<{ partNumber: number; etag: string; size: number }> }>('GET', `/files/${fileId}/parts`),
  reportProgress: (fileId: string, partsDone: number, totalParts: number) =>
    request<void>('POST', `/files/${fileId}/progress`, { partsDone, totalParts }),
  getTask: (taskId: string) => request<TaskDetail>('GET', `/tasks/${taskId}`),
  getHistory: (taskId: string) => request<{ events: TaskEvent[] }>('GET', `/tasks/${taskId}/history`),
  retryTask: (taskId: string) => request<TaskDetail>('POST', `/tasks/${taskId}/retry`),
  systemStatus: () => request<SystemStatus>('GET', '/system/status'),
};

export function eventsUrl(batchId: string, lastEventId: number): string {
  return `${API_URL}/batches/${batchId}/events?token=${encodeURIComponent(currentUser())}&lastEventId=${lastEventId}`;
}
