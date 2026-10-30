// Remembers in-progress upload actions in IndexedDB so a reload can resume instead of starting over.
// Re-selecting the same files reuses the stored Idempotency-Key, so POST /batches returns the same batch and
// multipart upload ids; GET /files/:id/parts then tells us which parts S3 already has.

export interface PendingUpload {
  signature: string; // name|size|lastModified of every file in the action
  idempotencyKey: string;
  batchId?: string;
  fileNames: string[];
  createdAt: number;
}

const DB = 'file-pipeline';
const STORE = 'pending-uploads';

function open(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB, 1);
    req.onupgradeneeded = () => req.result.createObjectStore(STORE, { keyPath: 'signature' });
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function tx<T>(mode: IDBTransactionMode, fn: (s: IDBObjectStore) => IDBRequest<T>): Promise<T | undefined> {
  try {
    const db = await open();
    return await new Promise<T>((resolve, reject) => {
      const req = fn(db.transaction(STORE, mode).objectStore(STORE));
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  } catch {
    return undefined; // private mode / blocked storage: uploads still work, just without resume
  }
}

export function signatureOf(files: File[]): string {
  return files.map((f) => `${f.name}|${f.size}|${f.lastModified}`).sort().join('\n');
}

export const resumeStore = {
  get: (signature: string) => tx<PendingUpload>('readonly', (s) => s.get(signature)),
  put: (p: PendingUpload) => tx('readwrite', (s) => s.put(p)),
  remove: (signature: string) => tx('readwrite', (s) => s.delete(signature)),
  all: async () => (await tx<PendingUpload[]>('readonly', (s) => s.getAll())) ?? [],
};
