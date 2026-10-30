/// <reference lib="webworker" />
// SHA-256 of a File, streamed in 8 MB chunks so a 100 MB file never sits in memory and the UI never freezes.
import { createSHA256 } from 'hash-wasm';

const CHUNK = 8 * 1024 * 1024;

self.onmessage = async (e: MessageEvent<{ file: File }>) => {
  const { file } = e.data;
  try {
    const hasher = await createSHA256();
    hasher.init();
    for (let offset = 0; offset < file.size; offset += CHUNK) {
      const buf = new Uint8Array(await file.slice(offset, offset + CHUNK).arrayBuffer());
      hasher.update(buf);
      self.postMessage({ type: 'progress', loaded: Math.min(offset + CHUNK, file.size) });
    }
    self.postMessage({ type: 'done', hex: hasher.digest('hex') });
  } catch (err) {
    self.postMessage({ type: 'error', message: String(err) });
  }
};
