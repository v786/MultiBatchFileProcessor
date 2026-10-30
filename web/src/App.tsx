import { useCallback, useEffect, useState } from 'react';
import { api, currentUser, type BatchListItem } from './api';
import { BatchView } from './components/BatchView';
import { SystemBar } from './components/SystemBar';
import { UploadPanel, type LocalProgress } from './components/UploadPanel';
import type { InAppNotice } from './components/useBatch';
import { clock, label, tone } from './format';

export function App() {
  const [user, setUser] = useState(currentUser());
  const [batches, setBatches] = useState<BatchListItem[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [local, setLocal] = useState<LocalProgress>({});
  const [notices, setNotices] = useState<Array<InAppNotice & { id: number }>>([]);

  const loadBatches = useCallback(() => {
    api.listBatches().then((r) => setBatches(r.batches), () => undefined);
  }, []);

  useEffect(() => {
    loadBatches();
    const t = window.setInterval(loadBatches, 5000);
    return () => window.clearInterval(t);
  }, [loadBatches, user]);

  const switchUser = (u: string) => {
    const clean = u.replace(/[^A-Za-z0-9_.@-]/g, '').slice(0, 64);
    try { localStorage.setItem('pipeline.user', clean || 'alice'); } catch { /* storage blocked */ }
    setUser(clean || 'alice');
    setSelected(null);
    setBatches([]);
  };

  const notice = useCallback((n: InAppNotice) => {
    const id = Date.now();
    setNotices((all) => [...all, { ...n, id }]);
    window.setTimeout(() => setNotices((all) => all.filter((x) => x.id !== id)), 12_000);
    loadBatches();
  }, [loadBatches]);

  return (
    <div className="app">
      <header className="top">
        <h1>File Pipeline</h1>
        <SystemBar />
        <label className="user">
          User
          <input value={user} onChange={(e) => switchUser(e.target.value)} aria-label="Stub user token" />
        </label>
      </header>

      <main className="layout">
        <div className="side">
          <UploadPanel
            onBatchCreated={(b) => { setSelected(b.batchId); loadBatches(); }}
            onLocalProgress={setLocal}
          />
          <section className="card">
            <h2>Your batches</h2>
            {batches.length === 0 && <p className="muted">No batches yet.</p>}
            <ul className="batch-list">
              {batches.map((b) => (
                <li key={b.batchId}>
                  <button className={`batch-item ${selected === b.batchId ? 'active' : ''}`} onClick={() => setSelected(b.batchId)}>
                    <span className="mono small">{b.batchId.slice(0, 16)}…</span>
                    <span className={`pill ${tone(b.status)}`}>{label(b.status)}</span>
                    <span className="muted small">{b.fileCount} files · {clock(b.createdAt)}</span>
                  </button>
                </li>
              ))}
            </ul>
          </section>
        </div>
        <div className="main">
          {selected
            ? <BatchView key={`${user}:${selected}`} batchId={selected} local={local} onNotice={notice} />
            : <section className="card empty"><p className="muted">Upload a batch or pick one to follow its progress live.</p></section>}
        </div>
      </main>

      <div className="toasts" aria-live="polite">
        {notices.map((n) => (
          <div key={n.id} className="toast">
            <strong>{n.title}</strong>
            <p>{n.body}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
