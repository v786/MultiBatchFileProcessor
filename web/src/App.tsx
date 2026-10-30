import { useCallback, useEffect, useState } from 'react';
import { api, currentUser, type BatchListItem } from './api';
import { BatchView } from './components/BatchView';
import { SystemBar } from './components/SystemBar';
import { UploadPanel, type LocalProgress } from './components/UploadPanel';
import type { InAppNotice } from './components/useBatch';
import { clock, label, tone } from './format';

// The selected batch lives in the URL hash (#/batches/<id>) so a refresh or the back button keeps the view.
function batchFromHash(): string | null {
  const m = /^#\/batches\/([A-Za-z0-9_-]+)$/.exec(window.location.hash);
  return m ? m[1] : null;
}

function useSelectedBatch(): [string | null, (id: string | null) => void] {
  const [selected, setSelected] = useState(batchFromHash);
  useEffect(() => {
    const onHash = () => setSelected(batchFromHash());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);
  const select = useCallback((id: string | null) => {
    window.location.hash = id ? `/batches/${id}` : '';
    setSelected(id);
  }, []);
  return [selected, select];
}

function cleanUser(u: string): string {
  return u.replace(/[^A-Za-z0-9_.@-]/g, '').slice(0, 64) || 'alice';
}

export function App() {
  const [user, setUser] = useState(currentUser());
  const [userDraft, setUserDraft] = useState(user);
  const [batches, setBatches] = useState<BatchListItem[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [selected, setSelected] = useSelectedBatch();
  const [local, setLocal] = useState<LocalProgress>({});
  const [notices, setNotices] = useState<Array<InAppNotice & { id: number }>>([]);

  const loadBatches = useCallback(() => {
    api.listBatches().then((r) => { setBatches(r.batches); setLoaded(true); }, () => undefined);
  }, []);

  useEffect(() => {
    loadBatches();
    const t = window.setInterval(loadBatches, 5000);
    return () => window.clearInterval(t);
  }, [loadBatches, user]);

  // Applied on Enter / blur, not per keystroke: switching user resets the whole view.
  const commitUser = () => {
    const next = cleanUser(userDraft);
    setUserDraft(next);
    if (next === user) return;
    try { localStorage.setItem('pipeline.user', next); } catch { /* storage blocked */ }
    setUser(next);
    setSelected(null);
    setBatches([]);
    setLoaded(false);
    setLocal({});
  };

  const notice = useCallback((n: InAppNotice) => {
    const id = Date.now() + Math.random();
    setNotices((all) => [...all, { ...n, id }]);
    window.setTimeout(() => setNotices((all) => all.filter((x) => x.id !== id)), 12_000);
    loadBatches();
  }, [loadBatches]);

  return (
    <div className="app">
      <header className="top">
        <h1>File Pipeline</h1>
        <SystemBar />
        <form className="user" onSubmit={(e) => { e.preventDefault(); commitUser(); }}>
          <label htmlFor="user">User</label>
          <input id="user" value={userDraft} onChange={(e) => setUserDraft(e.target.value)} onBlur={commitUser}
            aria-label="Stub user token" />
        </form>
      </header>

      <main className="layout">
        <div className="side">
          <UploadPanel
            key={user}
            onBatchCreated={(b) => { setSelected(b.batchId); loadBatches(); }}
            onLocalProgress={setLocal}
          />
          <section className="card">
            <div className="card-head">
              <h2>Your batches</h2>
              {selected && <button className="link" onClick={() => setSelected(null)}>Clear selection</button>}
            </div>
            {!loaded && <p className="muted">Loading…</p>}
            {loaded && batches.length === 0 && <p className="muted">No batches yet. Upload files above to start one.</p>}
            <ul className="batch-list">
              {batches.map((b) => {
                const failed = Object.entries(b.counts).filter(([s]) => tone(s) === 'bad').reduce((n, [, c]) => n + c, 0);
                const done = b.counts.DONE ?? 0;
                return (
                  <li key={b.batchId}>
                    <button className={`batch-item ${selected === b.batchId ? 'active' : ''}`} onClick={() => setSelected(b.batchId)}
                      aria-current={selected === b.batchId}>
                      <span className="mono small">{b.batchId.slice(0, 16)}…</span>
                      <span className={`pill ${tone(b.status)}`}>{label(b.status)}</span>
                      <span className="muted small">
                        {b.fileCount} files · {done} done{failed > 0 ? ` · ${failed} failed` : ''} · {clock(b.createdAt)}
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          </section>
        </div>
        <div className="main">
          {selected
            ? <BatchView key={`${user}:${selected}`} batchId={selected} local={local} onNotice={notice} />
            : (
              <section className="card empty">
                <ol className="howto">
                  <li><strong>Choose files</strong>: PDF, CSV, XLSX or JSON, up to 50 per batch.</li>
                  <li><strong>Pick notifications</strong>: in app, email or webhook.</li>
                  <li><strong>Upload</strong>: files go straight to storage, then are extracted and delivered.</li>
                  <li><strong>Follow progress</strong> here live; click any file for its history and retry options.</li>
                </ol>
              </section>
            )}
        </div>
      </main>

      <div className="toasts" aria-live="polite">
        {notices.map((n) => (
          <div key={n.id} className="toast">
            <button className="close" aria-label="Dismiss" onClick={() => setNotices((all) => all.filter((x) => x.id !== n.id))}>×</button>
            <strong>{n.title}</strong>
            <p>{n.body}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
