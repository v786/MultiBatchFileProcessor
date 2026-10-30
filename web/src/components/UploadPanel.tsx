import { useEffect, useRef, useState } from 'react';
import { ApiError, type CreatedBatch, type NotifySpec } from '../api';
import { bytes } from '../format';
import { resumeStore, type PendingUpload } from '../upload/resumeStore';
import { uploadBatch } from '../upload/uploadManager';

export interface LocalProgress { [fileId: string]: { loaded: number; total: number; failed?: string } }

const ACCEPT = '.pdf,.csv,.xlsx,.json';
const MAX_FILES = 50;

export function UploadPanel(props: {
  onBatchCreated: (batch: CreatedBatch) => void;
  onLocalProgress: (p: LocalProgress) => void;
}) {
  const [files, setFiles] = useState<File[]>([]);
  const [phase, setPhase] = useState<'idle' | 'hashing' | 'uploading'>('idle');
  const [hashPct, setHashPct] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [channels, setChannels] = useState<Record<'in_app' | 'email' | 'webhook', boolean>>({ in_app: true, email: false, webhook: false });
  const [webhookUrl, setWebhookUrl] = useState('http://mock-vendor:4000/webhooks');
  const [firstFailure, setFirstFailure] = useState(false);
  const [pending, setPending] = useState<PendingUpload[]>([]);
  const [dragging, setDragging] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const progress = useRef<LocalProgress>({});

  useEffect(() => { void resumeStore.all().then(setPending); }, [phase]);

  const pick = (list: FileList | null) => {
    if (!list) return;
    const picked = Array.from(list);
    setError(picked.length > MAX_FILES ? `At most ${MAX_FILES} files per batch; ${picked.length} selected.` : null);
    setFiles(picked.slice(0, MAX_FILES));
    setNotice(null);
  };

  const start = async () => {
    setError(null);
    setNotice(null);
    setPhase('hashing');
    progress.current = {};
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
    try {
      const { failures } = await uploadBatch(files, {
        notify,
        cb: {
          onHashProgress: (i, l) => {
            loaded[i] = l;
            setHashPct(Math.round((100 * loaded.reduce((a, b) => a + b, 0)) / total));
          },
          onBatchCreated: (batch, resumed) => {
            setPhase('uploading');
            if (resumed) setNotice('Resuming an interrupted upload: only missing parts will be sent.');
            const rejected = batch.tasks.filter((t) => t.status === 'REJECTED').length;
            if (rejected) setNotice(`${rejected} file(s) rejected; the rest of the batch continues.`);
            props.onBatchCreated(batch);
          },
          onUploadProgress: (fileId, l, t) => {
            progress.current = { ...progress.current, [fileId]: { loaded: l, total: t } };
            props.onLocalProgress(progress.current);
          },
          onFileFailed: (fileId, err) => {
            progress.current = { ...progress.current, [fileId]: { ...(progress.current[fileId] ?? { loaded: 0, total: 0 }), failed: String(err) } };
            props.onLocalProgress(progress.current);
          },
        },
      });
      setFiles([]);
      if (input.current) input.current.value = '';
      if (failures) setError(`${failures} file(s) could not be uploaded. Select the same files again to resume.`);
    } catch (err) {
      if (err instanceof ApiError && err.status === 422 && typeof err.body === 'object' && err.body && 'files' in err.body) {
        const reasons = (err.body as { files: Array<{ fileName: string; message: string }> }).files;
        setError(reasons.map((r) => `${r.fileName}: ${r.message}`).join('\n'));
      } else {
        setError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      setPhase('idle');
      setHashPct(0);
    }
  };

  const totalBytes = files.reduce((n, f) => n + f.size, 0);
  const busy = phase !== 'idle';

  return (
    <section className="card">
      <h2>New batch</h2>
      {pending.length > 0 && !busy && (
        <div className="banner">
          Interrupted upload: {pending[0].fileNames.slice(0, 3).join(', ')}{pending[0].fileNames.length > 3 ? '…' : ''}.
          Select the same files again to resume.
          <button className="link" onClick={() => { void Promise.all(pending.map((p) => resumeStore.remove(p.signature))).then(() => setPending([])); }}>Discard</button>
        </div>
      )}
      <label
        className={`drop ${dragging ? 'drag' : ''}`}
        onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => { e.preventDefault(); setDragging(false); pick(e.dataTransfer.files); }}
      >
        <input ref={input} type="file" multiple accept={ACCEPT} disabled={busy} onChange={(e) => pick(e.target.files)} />
        <strong>Drop files or click to choose</strong>
        <span className="muted">PDF, CSV, XLSX, JSON · up to 100 MB each · {MAX_FILES} per batch</span>
      </label>
      {files.length > 0 && (
        <p className="muted">{files.length} file(s), {bytes(totalBytes)}</p>
      )}
      <fieldset className="notify">
        <legend>Tell me when the batch is done</legend>
        {(['in_app', 'email', 'webhook'] as const).map((c) => (
          <label key={c}>
            <input type="checkbox" checked={channels[c]} disabled={busy}
              onChange={(e) => setChannels({ ...channels, [c]: e.target.checked })} />
            {c === 'in_app' ? 'In app' : c === 'email' ? 'Email (MailHog :8025)' : 'Webhook'}
          </label>
        ))}
        {channels.webhook && (
          <input className="text" value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} placeholder="https://…" />
        )}
        <label>
          <input type="checkbox" checked={firstFailure} disabled={busy} onChange={(e) => setFirstFailure(e.target.checked)} />
          Also alert on the first failure
        </label>
      </fieldset>
      <button className="primary" disabled={busy || files.length === 0} onClick={() => void start()}>
        {phase === 'hashing' ? `Hashing… ${hashPct}%` : phase === 'uploading' ? 'Uploading…' : 'Upload batch'}
      </button>
      {notice && <p className="notice">{notice}</p>}
      {error && <p className="error">{error}</p>}
    </section>
  );
}
