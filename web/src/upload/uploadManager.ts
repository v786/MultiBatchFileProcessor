// Batch upload: hash -> create batch (idempotent) -> upload to S3 with a bounded pool -> complete each file.
import { api, ApiError, type CreatedBatch, type NotifySpec } from '../api';
import { hashInWorker } from './hash';
import { putWithRetry, uploadParts } from './multipart';
import { resumeStore, signatureOf } from './resumeStore';

export const FILE_CONCURRENCY = 4;
export const PART_CONCURRENCY = 3;
export const RETRIES = 5;
const HASH_CONCURRENCY = 3;

export interface UploadCallbacks {
  onHashProgress?: (index: number, loaded: number) => void;
  onBatchCreated?: (batch: CreatedBatch, resumed: boolean) => void;
  onUploadProgress?: (fileId: string, loaded: number, total: number) => void;
  onFileUploaded?: (fileId: string) => void;
  onFileFailed?: (fileId: string, err: unknown) => void;
}

/** Tiny p-limit: run at most `n` async functions at once. */
export function pLimit(n: number) {
  let active = 0;
  const queue: Array<() => void> = [];
  const next = () => {
    if (active >= n || queue.length === 0) return;
    active++;
    queue.shift()!();
  };
  return <T>(fn: () => Promise<T>) =>
    new Promise<T>((resolve, reject) => {
      queue.push(() => fn().then(resolve, reject).finally(() => { active--; next(); }));
      next();
    });
}

async function createBatchWithRetry(...args: Parameters<typeof api.createBatch>): Promise<CreatedBatch> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await api.createBatch(...args);
    } catch (err) {
      // Same Idempotency-Key on every try: a retry after a lost response returns the same batch.
      const retriable = !(err instanceof ApiError) || err.status >= 500 || err.status === 429;
      if (!retriable || attempt >= 3) throw err;
      const wait = err instanceof ApiError && err.retryAfter ? err.retryAfter * 1000 : 1000 * 2 ** attempt;
      await new Promise((r) => setTimeout(r, wait));
    }
  }
}

export async function uploadBatch(files: File[], opts: { notify?: NotifySpec; cb?: UploadCallbacks; signal?: AbortSignal } = {}) {
  const cb = opts.cb ?? {};
  const hashPool = pLimit(HASH_CONCURRENCY);
  const hashes = await Promise.all(files.map((f, i) => hashPool(() => hashInWorker(f, (l) => cb.onHashProgress?.(i, l)))));

  const signature = signatureOf(files);
  const pending = await resumeStore.get(signature);
  const idempotencyKey = pending?.idempotencyKey ?? crypto.randomUUID(); // generated once per user action
  await resumeStore.put({ signature, idempotencyKey, fileNames: files.map((f) => f.name), createdAt: Date.now() });

  const batch = await createBatchWithRetry(
    files.map((f, i) => ({ name: f.name, size: f.size, mimeType: f.type, sha256: hashes[i] })),
    { idempotencyKey, notify: opts.notify },
  );
  await resumeStore.put({ signature, idempotencyKey, batchId: batch.batchId, fileNames: files.map((f) => f.name), createdAt: Date.now() });
  cb.onBatchCreated?.(batch, Boolean(pending));

  const filePool = pLimit(FILE_CONCURRENCY);
  let failures = 0;
  await Promise.allSettled(batch.tasks.map((task, i) => filePool(async () => {
    const plan = task.upload;
    if (!plan || task.duplicateOf) return; // rejected, deduped by hash, or already uploaded
    const file = files[i];
    try {
      let parts: Array<{ partNumber: number; etag: string }> = [];
      if (plan.multipart) {
        const done = new Map<number, string>();
        if (pending) {
          // Resume: only upload the parts S3 does not have yet.
          for (const p of (await api.getParts(task.fileId)).parts) done.set(p.partNumber, p.etag);
        }
        parts = await uploadParts(file, plan, {
          partConcurrency: PART_CONCURRENCY,
          retries: RETRIES,
          done,
          signal: opts.signal,
          onProgress: (loaded) => cb.onUploadProgress?.(task.fileId, loaded, file.size),
          onPartDone: (n) => { void api.reportProgress(task.fileId, n, plan.partCount).catch(() => undefined); },
          refresh: async (partNumber) => {
            const fresh = await api.refreshUrls(task.fileId, [partNumber]);
            if (!fresh.multipart) throw new Error('unexpected single-PUT plan');
            return fresh.parts[0].url;
          },
        });
      } else {
        await putWithRetry(plan.url, file, plan.headers, {
          retries: RETRIES,
          signal: opts.signal,
          onProgress: (loaded) => cb.onUploadProgress?.(task.fileId, loaded, file.size),
          refresh: async () => {
            const fresh = await api.refreshUrls(task.fileId);
            if (fresh.multipart) throw new Error('unexpected multipart plan');
            return fresh.url;
          },
        });
      }
      await api.completeFile(task.fileId, parts);
      cb.onFileUploaded?.(task.fileId);
    } catch (err) {
      failures++;
      cb.onFileFailed?.(task.fileId, err); // other files continue
    }
  })));

  if (failures === 0) await resumeStore.remove(signature); // otherwise keep it: re-selecting the files resumes
  return { batch, failures };
}
