import { useEffect, useRef, useState } from 'react';
import { ApiError, type CreatedBatch, type NotifySpec } from '../api';
import { bytes } from '../format';
import { resumeStore, type PendingUpload } from '../upload/resumeStore';
import { uploadBatch } from '../upload/uploadManager';

export interface LocalProgress { [fileId: string]: { loaded: number; total: number; failed?: string } }

const EXTENSIONS = ['pdf', 'csv', 'xlsx', 'json'];
const ACCEPT = EXTENSIONS.map((e) => `.${e}`).join(',');
const MAX_FILES = 50;
const MAX_FILE_BYTES = 100 * 1024 * 1024;

type Phase = 'select' | 'hashing' | 'uploading' | 'done';

/** Client-side mirror of the server's checks, so problems show up before anything is sent. */
function problemWith(f: File): string | null {
  const ext = f.name.split('.').pop()?.toLowerCase() ?? '';
  if (!EXTENSIONS.includes(ext)) return 'unsupported type';
  if (f.size === 0) return 'empty file';
  if (f.size > MAX_FILE_BYTES) return `larger than ${bytes(MAX_FILE_BYTES)}`;
  return null;
}

const keyOf = (f: File) => `${f.name}|${f.size}|${f.lastModified}`;

export function UploadPanel(props: {
  onBatchCreated: (batch: CreatedBatch) => void;
  onLocalProgress: (p: LocalProgress) => void;
}) {
  const [files, setFiles] = useState<File[]>([]);
  const [phase, setPhase] = useState<Phase>('select');
  const [hashPct, setHashPct] = useState(0);
  const [uploaded, setUploaded] = useState({ done: 0, total: 0 });
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [channels, setChannels] = useState<Record<'in_app' | 'email' | 'webhook', boolean>>({ in_app: true, email: false, webhook: false });
  const [webhookUrl, setWebhookUrl] = useState('http://mock-vendor:4000/webhooks');
  const [firstFailure, setFirstFailure] = useState(false);
  const [pending, setPending] = useState<PendingUpload[]>([]);
  const [dragging, setDragging] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const progress = useRef<LocalProgress>({});
  const abort = useRef<AbortController | null>(null);

  useEffect(() => { void resumeStore.all().then(setPending); }, [phase]);

  // Picking again adds to the selection (same file twice is ignored) instead of replacing it.
  const pick = (list: FileList | null) => {
    if (!list || list.length === 0) return;
    const seen = new Set(files.map(keyOf));
    const merged = [...files, ...Array.from(list).filter((f) => !seen.has(keyOf(f)))];
    setError(merged.length > MAX_FILES ? `At most ${MAX_FILES} files per batch; only the first ${MAX_FILES} were kept.` : null);
    setFiles(merged.slice(0, MAX_FILES));
    setNotice(null);
    if (phase === 'done') setPhase('select');
    if (input.current) input.current.value = '';
  };

  const remove = (f: File) => setFiles((all) => all.filter((x) => x !== f));
  const invalid = files.filter((f) => problemWith(f));

  const reset = () => {
    setFiles([]);
    setError(null);
    setNotice(null);
    setPhase('select');
  };

  const start = async () => {
    setError(null);
    setNotice(null);
    setPhase('hashing');
    progress.current = {};
    abort.current = new AbortController();
    const loaded = new Array(files.length).fill(0);
    const total = files.reduce((n, f) => n + f.size, 0) || 1;
    const notify: NotifySpec = {
      channels: (Object.keys(channels) as Array<keyof typeof channels>).filter((c) => channels[c]),
      ...(channels.webhook ? { webhookUrl } : {}),
      ...(firstFailure ? { notifyOn: ['first_failure'] } : {}),
    };
    if (channels.in_app && 'Notification' in window && Notification.permission === 'default') {
      void Notification.requestPermission();
    }
    let finished = false;
    try {
      const { batch, failures } = await uploadBatch(files, {
        notify,
        signal: abort.current.signal,
        cb: {
          onHashProgress: (i, l) => {
            loaded[i] = l;
            setHashPct(Math.round((100 * loaded.reduce((a, b) => a + b, 0)) / total));
          },
          onBatchCreated: (b, resumed) => {
            setPhase('uploading');
            setUploaded({ done: 0, total: b.tasks.filter((t) => t.upload && !t.duplicateOf).length });
            const notes: string[] = [];
            if (resumed) notes.push('Resuming an interrupted upload: only missing parts will be sent.');
            const rejected = b.tasks.filter((t) => t.status === 'REJECTED').length;
            if (rejected) notes.push(`${rejected} file(s) rejected; the rest of the batch continues.`);
            const dupes = b.tasks.filter((t) => t.duplicateOf).length;
            if (dupes) notes.push(`${dupes} file(s) already uploaded earlier; not sent again.`);
            setNotice(notes.join(' ') || null);
            props.onBatchCreated(b);
          },
          onUploadProgress: (fileId, l, t) => {
            progress.current = { ...progress.current, [fileId]: { loaded: l, total: t } };
            props.onLocalProgress(progress.current);
          },
          onFileUploaded: () => setUploaded((u) => ({ ...u, done: u.done + 1 })),
          onFileFailed: (fileId, err) => {
            const reason = err instanceof DOMException && err.name === 'AbortError' ? 'cancelled' : String(err);
            progress.current = { ...progress.current, [fileId]: { ...(progress.current[fileId] ?? { loaded: 0, total: 0 }), failed: reason } };
            props.onLocalProgress(progress.current);
          },
        },
      });
      finished = true;
      setFiles([]);
      if (abort.current?.signal.aborted) {
        setError(`Upload cancelled. Select the same files again to resume batch ${batch.batchId}.`);
      } else if (failures) {
        setError(`${failures} file(s) could not be uploaded. Select the same files again to resume.`);
      }
    } catch (err) {
      if (err instanceof ApiError && err.status === 422 && typeof err.body === 'object' && err.body && 'files' in err.body) {
        const reasons = (err.body as { files: Array<{ fileName: string; message: string }> }).files;
        setError(reasons.map((r) => `${r.fileName}: ${r.message}`).join('\n'));
      } else {
        setError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      abort.current = null;
      setPhase(finished ? 'done' : 'select');
      setHashPct(0);
    }
  };

  const totalBytes = files.reduce((n, f) => n + f.size, 0);
  const busy = phase === 'hashing' || phase === 'uploading';
  const step = busy ? 3 : files.length > 0 ? 2 : 1;

  return (
    <section className="card">
      <h2>New batch</h2>
      <ol className="steps" aria-label="Upload steps">
        {['Choose files', 'Notifications', 'Upload'].map((s, i) => (
          <li key={s} className={i + 1 < step ? 'past' : i + 1 === step ? 'current' : ''}>{s}</li>
        ))}
      </ol>

      {pending.length > 0 && !busy && (
        <div className="banner">
          Interrupted upload: {pending[0].fileNames.slice(0, 3).join(', ')}{pending[0].fileNames.length > 3 ? '…' : ''}.
          Select the same files again to resume.
          <button className="link" onClick={() => { void Promise.all(pending.map((p) => resumeStore.remove(p.signature))).then(() => setPending([])); }}>Discard</button>
        </div>
      )}

      <label
        className={`drop ${dragging ? 'drag' : ''} ${busy ? 'disabled' : ''}`}
        onDragOver={(e) => { e.preventDefault(); if (!busy) setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => { e.preventDefault(); setDragging(false); if (!busy) pick(e.dataTransfer.files); }}
      >
        <input ref={input} type="file" multiple accept={ACCEPT} disabled={busy} onChange={(e) => pick(e.target.files)} />
        <strong>{files.length ? 'Add more files' : 'Drop files or click to choose'}</strong>
        <span className="muted">PDF, CSV, XLSX, JSON · up to {bytes(MAX_FILE_BYTES)} each · {MAX_FILES} per batch</span>
      </label>

      {files.length > 0 && (
        <div className="picked">
          <div className="card-head">
            <span className="muted">{files.length} file(s), {bytes(totalBytes)}</span>
            {!busy && <button className="link" onClick={() => setFiles([])}>Clear all</button>}
          </div>
          <ul className="file-list">
            {files.map((f) => {
              const problem = problemWith(f);
              return (
                <li key={keyOf(f)} className={problem ? 'invalid' : ''}>
                  <span className="fname" title={f.name}>{f.name}</span>
                  <span className="muted small">{problem ?? bytes(f.size)}</span>
                  {!busy && <button className="remove" onClick={() => remove(f)} aria-label={`Remove ${f.name}`}>×</button>}
                </li>
              );
            })}
          </ul>
          {invalid.length > 0 && !busy && (
            <p className="error small">
              {invalid.length} file(s) can't be processed.
              <button className="link" onClick={() => setFiles((all) => all.filter((f) => !problemWith(f)))}>Remove them</button>
            </p>
          )}
        </div>
      )}

      <fieldset className="notify" disabled={busy}>
        <legend>Tell me when the batch is done</legend>
        {(['in_app', 'email', 'webhook'] as const).map((c) => (
          <label key={c}>
            <input type="checkbox" checked={channels[c]} onChange={(e) => setChannels({ ...channels, [c]: e.target.checked })} />
            {c === 'in_app' ? 'In app' : c === 'email' ? 'Email (MailHog :8025)' : 'Webhook'}
          </label>
        ))}
        {channels.webhook && (
          <input className="text" type="url" value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} placeholder="https://…"
            aria-label="Webhook URL" />
        )}
        <label>
          <input type="checkbox" checked={firstFailure} onChange={(e) => setFirstFailure(e.target.checked)} />
          Also alert on the first failure
        </label>
      </fieldset>

      {busy ? (
        <div className="upload-status">
          <div className="bar"><div style={{ width: `${phase === 'hashing' ? hashPct : Math.round((100 * uploaded.done) / Math.max(1, uploaded.total))}%` }} /></div>
          <div className="row">
            <span className="muted small">
              {phase === 'hashing' ? `Preparing files… ${hashPct}%` : `Uploaded ${uploaded.done} of ${uploaded.total} file(s)`}
            </span>
            <button className="secondary" disabled={phase !== 'uploading'} onClick={() => abort.current?.abort()}>Cancel</button>
          </div>
        </div>
      ) : (
        <button className="primary" disabled={files.length === 0 || invalid.length > 0 || (channels.webhook && !webhookUrl)}
          onClick={() => void start()}>
          {files.length ? `Upload ${files.length} file(s)` : 'Upload batch'}
        </button>
      )}

      {phase === 'done' && !error && (
        <p className="success">
          Upload complete. Extraction and delivery continue on the server; progress is shown on the right.
          <button className="link" onClick={reset}>Start another batch</button>
        </p>
      )}
      {notice && <p className="notice">{notice}</p>}
      {error && <p className="error">{error}</p>}
    </section>
  );
}
