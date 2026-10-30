export function hashInWorker(file: File, onProgress?: (loaded: number) => void): Promise<string> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./hashWorker.ts', import.meta.url), { type: 'module' });
    worker.onmessage = (e: MessageEvent<{ type: string; loaded?: number; hex?: string; message?: string }>) => {
      if (e.data.type === 'progress') onProgress?.(e.data.loaded ?? 0);
      else {
        worker.terminate();
        if (e.data.type === 'done') resolve(e.data.hex!);
        else reject(new Error(e.data.message));
      }
    };
    worker.onerror = (e) => {
      worker.terminate();
      reject(new Error(e.message));
    };
    worker.postMessage({ file });
  });
}
