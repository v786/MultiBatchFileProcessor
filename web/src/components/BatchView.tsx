import { useEffect, useState } from 'react';
import { api, type TaskSummary } from '../api';
import { bytes, clock, label, secondsAgo, tone } from '../format';
import { TaskDrawer } from './TaskDrawer';
import type { LocalProgress } from './UploadPanel';
import { useBatch, type InAppNotice } from './useBatch';

function useNow(intervalMs = 1000) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(t);
  }, [intervalMs]);
  return now;
}

/** The server's status line plus the parts that depend on the viewer's clock. */
export function statusLine(t: TaskSummary, now: number): string {
  const s = t.statusText;
  if (s.startedAt && t.deliveryStatus === 'IN_FLIGHT') return `${s.text} (started ${secondsAgo(s.startedAt, now)} s ago)`;
  if (s.retryAt) {
    const paused = s.text.startsWith('Vendor unavailable');
    return `${s.text}${paused ? ' until' : ', next attempt'} ${clock(s.retryAt)}`;
  }
  return s.text;
}

const FILTERS = {
  all: { label: 'All', match: () => true },
  active: { label: 'In progress', match: (t: TaskSummary) => tone(t.status) === 'busy' || tone(t.status) === 'idle' },
  failed: { label: 'Failed', match: (t: TaskSummary) => tone(t.status) === 'bad' },
  done: { label: 'Done', match: (t: TaskSummary) => tone(t.status) === 'ok' },
} as const;
type Filter = keyof typeof FILTERS;

const FINAL = new Set(['COMPLETED', 'PARTIAL', 'FAILED']);

export function BatchView(props: { batchId: string; local: LocalProgress; onNotice: (n: InAppNotice) => void }) {
  const { batch, live, error, refetch } = useBatch(props.batchId, props.onNotice);
  const [openTask, setOpenTask] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [filter, setFilter] = useState<Filter>('all');
  const now = useNow();

  if (error && !batch) return <section className="card"><p className="error">{error}</p></section>;
  if (!batch) return <section className="card"><p className="muted">Loading batch…</p></section>;

  const failedRetryable = batch.tasks.filter((t) => t.actions.includes('retry_delivery') || t.actions.includes('retry_extract')).length;

  const shown = batch.tasks.filter(FILTERS[filter].match);

  const retryAll = async () => {
    setBusy(true);
    try {
      await api.retryFailed(batch.batchId);
      await refetch();
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="card batch">
      <header className="batch-head">
        <div>
          <h2><code>{batch.batchId}</code></h2>
          <p className="muted">
            {batch.fileCount} files · created {clock(batch.createdAt)}
            {batch.completedAt && ` · finished ${clock(batch.completedAt)}`}
          </p>
        </div>
        <div className="head-right">
          <span className={`live ${live}`} title="Live updates">{live === 'live' ? '● live' : live === 'polling' ? '○ polling' : '◌ connecting'}</span>
          <span className={`pill ${tone(batch.status)}`}>{label(batch.status)}</span>
        </div>
      </header>

      <div className="bar big" role="progressbar" aria-valuenow={batch.progress} aria-valuemin={0} aria-valuemax={100}>
        <div style={{ width: `${batch.progress}%` }} />
      </div>
      <p className="summary">{batch.progress}% · {batch.summary}</p>

      {FINAL.has(batch.status) && (
        <div className={`banner ${batch.status === 'COMPLETED' ? 'ok' : ''}`}>
          {batch.status === 'COMPLETED'
            ? 'All files were processed and delivered.'
            : 'The batch finished with failures. Open a failed file to see why and what you can do.'}
        </div>
      )}

      <div className="toolbar">
        <div className="filters" role="tablist" aria-label="Filter files">
          {(Object.keys(FILTERS) as Filter[]).map((f) => (
            <button key={f} role="tab" aria-selected={filter === f} className={`chip ${filter === f ? 'active' : ''}`}
              onClick={() => setFilter(f)}>
              {FILTERS[f].label} <span className="muted">{batch.tasks.filter(FILTERS[f].match).length}</span>
            </button>
          ))}
        </div>
        {failedRetryable > 0 && (
          <button className="secondary" disabled={busy} onClick={() => void retryAll()}>
            {busy ? 'Retrying…' : `Retry ${failedRetryable} failed`}
          </button>
        )}
      </div>

      <div className="table-wrap">
        <table className="tasks">
          <thead>
            <tr><th>File</th><th>Status</th><th className="w-progress">Progress</th><th>What's happening</th></tr>
          </thead>
          <tbody>
            {shown.length === 0 && (
              <tr><td colSpan={4} className="muted">No files match this filter.</td></tr>
            )}
            {shown.map((t) => {
              const lp = props.local[t.fileId];
              const uploading = lp && !lp.failed && (t.status === 'PENDING_UPLOAD' || t.status === 'UPLOADING');
              const pct = uploading ? Math.round((40 * lp.loaded) / Math.max(1, lp.total)) : t.progress;
              return (
                <tr key={t.taskId} onClick={() => setOpenTask(t.taskId)} className="clickable" tabIndex={0}
                  onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setOpenTask(t.taskId); } }}
                  aria-label={`${t.fileName}: ${label(t.status)}. Open details`}>
                  <td>
                    <div className="fname" title={t.fileName}>{t.fileName}</div>
                    <div className="muted small"><code>{t.taskId}</code> · {bytes(t.sizeBytes)}</div>
                  </td>
                  <td>
                    <span className={`pill ${tone(t.status)}`}>{label(t.status)}</span>
                    <div className="muted small">{t.stage}</div>
                  </td>
                  <td>
                    <div className="bar"><div style={{ width: `${pct}%` }} /></div>
                    <div className="muted small">{pct}%</div>
                  </td>
                  <td className="what">
                    {lp?.failed ? <span className="error">Upload failed: {lp.failed}</span>
                      : uploading ? `Uploading ${bytes(lp.loaded)} of ${bytes(lp.total)}`
                        : statusLine(t, now)}
                    {t.warnings > 0 && <span className="muted small"> · {t.warnings} warning(s)</span>}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {openTask && <TaskDrawer taskId={openTask} onClose={() => setOpenTask(null)} onChanged={() => void refetch()} />}
    </section>
  );
}
