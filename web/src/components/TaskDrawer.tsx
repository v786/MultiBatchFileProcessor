import { useEffect, useState } from 'react';
import { api, type TaskDetail, type TaskEvent } from '../api';
import { clock, label, tone } from '../format';

const ACTION_TEXT: Record<string, string> = {
  retry_delivery: 'Retry delivery',
  retry_extract: 'Retry extraction',
  reupload: 'Upload the file again in a new batch',
  upload_different_file: 'Upload a different file',
};

export function TaskDrawer(props: { taskId: string; onClose: () => void; onChanged: () => void }) {
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [events, setEvents] = useState<TaskEvent[]>([]);
  const [busy, setBusy] = useState(false);

  const load = async () => {
    const [t, h] = await Promise.all([api.getTask(props.taskId), api.getHistory(props.taskId)]);
    setTask(t);
    setEvents(h.events);
  };

  useEffect(() => {
    void load();
    const t = window.setInterval(() => void load(), 3000);
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && props.onClose();
    window.addEventListener('keydown', onKey);
    return () => { window.clearInterval(t); window.removeEventListener('keydown', onKey); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.taskId]);

  const retry = async () => {
    setBusy(true);
    try {
      await api.retryTask(props.taskId);
      await load();
      props.onChanged();
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="drawer-backdrop" onClick={props.onClose}>
      <aside className="drawer" onClick={(e) => e.stopPropagation()} aria-label="Task details">
        <button className="close" onClick={props.onClose} aria-label="Close">×</button>
        {!task ? <p className="muted">Loading…</p> : (
          <>
            <h3 className="fname">{task.fileName}</h3>
            <p className="muted small"><code>{task.taskId}</code></p>
            <p><span className={`pill ${tone(task.status)}`}>{label(task.status)}</span> {task.progress}%</p>

            <ol className="stages">
              {(['upload', 'extract', 'deliver'] as const).map((name) => {
                const st = task.stages[name];
                return (
                  <li key={name} className={`stage ${st.status.toLowerCase()}`}>
                    <div className="stage-head">
                      <strong>{name}</strong>
                      <span className="muted small">{label(st.status)}{st.finishedAt ? ` · ${clock(st.finishedAt)}` : ''}</span>
                    </div>
                    {name === 'extract' && (st.warnings ?? 0) > 0 && <div className="small">{st.warnings} warning(s) in the data</div>}
                    {name === 'deliver' && (st.attempts ?? 0) > 0 && (
                      <div className="small">Attempt {st.attempts} of {st.maxAttempts}{st.vendorRef ? ` · vendor ref ${st.vendorRef}` : ''}</div>
                    )}
                    {st.error && (
                      <div className="cause">
                        <div><strong>{st.error.message}</strong></div>
                        <div className="muted small">{st.error.kind} · {st.error.code}</div>
                      </div>
                    )}
                  </li>
                );
              })}
            </ol>

            {task.actions.length > 0 && (
              <div className="actions">
                <h4>What you can do</h4>
                {task.actions.map((a) => (a === 'retry_delivery' || a === 'retry_extract'
                  ? <button key={a} className="primary" disabled={busy} onClick={() => void retry()}>{ACTION_TEXT[a]}</button>
                  : <p key={a} className="muted">{ACTION_TEXT[a]}</p>))}
              </div>
            )}

            <h4>History</h4>
            <ul className="timeline">
              {events.map((e) => (
                <li key={e.id}>
                  <span className="muted small">{clock(e.at)}</span>
                  <span className="small"> {e.stage} → <strong>{label(e.to)}</strong></span>
                  {e.message && <div className="small">{e.message}{e.nextRetryAt ? ` (next ${clock(e.nextRetryAt)})` : ''}</div>}
                  {e.errorCode && <div className="muted small">{e.errorKind} · {e.errorCode}</div>}
                </li>
              ))}
            </ul>
          </>
        )}
      </aside>
    </div>
  );
}
