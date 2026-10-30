// Uploads straight to S3 with presigned URLs. XHR (not fetch) so we get byte-level upload progress.

export class HttpError extends Error {
  constructor(public status: number) {
    super(`HTTP ${status}`);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Full jitter: random(0, min(cap, base * 2^attempt)). */
export function backoffMs(attempt: number, baseMs = 500, capMs = 15_000): number {
  return Math.random() * Math.min(capMs, baseMs * 2 ** attempt);
}

function putOnce(url: string, body: Blob, headers: Record<string, string>, onProgress: (loaded: number) => void,
  signal?: AbortSignal): Promise<string> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('PUT', url);
    for (const [k, v] of Object.entries(headers)) xhr.setRequestHeader(k, v);
    xhr.upload.onprogress = (e) => onProgress(e.loaded);
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve(xhr.getResponseHeader('ETag') ?? '');
      else reject(new HttpError(xhr.status));
    };
    xhr.onerror = () => reject(new HttpError(0)); // network error
    xhr.onabort = () => reject(new DOMException('aborted', 'AbortError'));
    signal?.addEventListener('abort', () => xhr.abort(), { once: true });
    xhr.send(body);
  });
}

interface RetryOpts {
  retries: number;
  onProgress: (loaded: number) => void;
  /** Called after a 403 (expired URL): returns a freshly signed URL. */
  refresh: () => Promise<string>;
  signal?: AbortSignal;
}

/** Retries network errors and 5xx with jittered backoff; a 403 re-signs the URL. */
export async function putWithRetry(url: string, body: Blob, headers: Record<string, string>, opts: RetryOpts): Promise<string> {
  let current = url;
  for (let attempt = 0; ; attempt++) {
    try {
      return await putOnce(current, body, headers, opts.onProgress, opts.signal);
    } catch (err) {
      if (!(err instanceof HttpError) || attempt >= opts.retries) throw err;
      opts.onProgress(0);
      if (err.status === 403) {
        current = await opts.refresh();
      } else if (err.status !== 0 && err.status < 500) {
        throw err; // 4xx other than 403: retrying will not help
      } else {
        await sleep(backoffMs(attempt));
      }
    }
  }
}

export interface MultipartPlan { uploadId: string; partSize: number; partCount: number; parts: Array<{ partNumber: number; url: string }> }

/** Uploads the parts not already in S3, `partConcurrency` at a time. Returns every part's ETag. */
export async function uploadParts(
  file: File,
  plan: MultipartPlan,
  opts: {
    partConcurrency: number;
    retries: number;
    done: Map<number, string>; // parts already uploaded (resume), filled in as parts finish
    onProgress: (loaded: number) => void;
    onPartDone?: (doneCount: number) => void;
    refresh: (partNumber: number) => Promise<string>;
    signal?: AbortSignal;
  },
): Promise<Array<{ partNumber: number; etag: string }>> {
  const urls = new Map(plan.parts.map((p) => [p.partNumber, p.url]));
  const todo = plan.parts.map((p) => p.partNumber).filter((n) => !opts.done.has(n));
  const inflightBytes = new Map<number, number>();
  const partBytes = (n: number) => Math.min(plan.partSize, file.size - (n - 1) * plan.partSize);
  const report = () => {
    let loaded = 0;
    for (const n of opts.done.keys()) loaded += partBytes(n);
    for (const b of inflightBytes.values()) loaded += b;
    opts.onProgress(loaded);
  };
  report();

  const worker = async () => {
    for (let n = todo.shift(); n !== undefined; n = todo.shift()) {
      const partNumber = n;
      const blob = file.slice((partNumber - 1) * plan.partSize, partNumber * plan.partSize);
      const etag = await putWithRetry(urls.get(partNumber)!, blob, {}, {
        retries: opts.retries,
        signal: opts.signal,
        onProgress: (loaded) => {
          inflightBytes.set(partNumber, loaded);
          report();
        },
        refresh: () => opts.refresh(partNumber),
      });
      inflightBytes.delete(partNumber);
      opts.done.set(partNumber, etag);
      opts.onPartDone?.(opts.done.size);
      report();
    }
  };
  await Promise.all(Array.from({ length: Math.min(opts.partConcurrency, todo.length) }, worker));
  return [...opts.done.entries()].sort(([a], [b]) => a - b).map(([partNumber, etag]) => ({ partNumber, etag }));
}
