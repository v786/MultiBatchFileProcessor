import { useEffect, useState } from 'react';
import { api, type SystemStatus } from '../api';
import { clock } from '../format';

/** Vendor slots in use, breaker state and the shared queue, refreshed every 2 s. */
export function SystemBar() {
  const [st, setSt] = useState<SystemStatus | null>(null);
  const [down, setDown] = useState(false);

  useEffect(() => {
    const load = () => api.systemStatus().then((s) => { setSt(s); setDown(false); }, () => setDown(true));
    void load();
    const t = window.setInterval(load, 2000);
    return () => window.clearInterval(t);
  }, []);

  if (down) return <div className="sysbar"><span className="error">API unreachable</span></div>;
  if (!st) return <div className="sysbar muted">…</div>;
  const { vendor, queue } = st;
  const breaker = vendor.breaker.state;
  return (
    <div className="sysbar">
      <span title="Vendor calls in flight / usable slots">
        Vendor slots <strong>{vendor.inflight}</strong>/{vendor.slots}
        <span className="slots" aria-hidden>
          {Array.from({ length: vendor.maxConcurrent }, (_, i) => (
            <i key={i} className={i < vendor.inflight ? 'on' : i >= vendor.slots ? 'off' : ''} />
          ))}
        </span>
      </span>
      <span className={`pill ${breaker === 'closed' ? 'ok' : 'bad'}`} title="Circuit breaker">
        breaker {breaker.replace('_', '-')}
        {breaker === 'open' && vendor.breaker.openUntil ? ` · probe ${clock(new Date(vendor.breaker.openUntil * 1000).toISOString())}` : ''}
      </span>
      <span>Waiting <strong>{queue.waiting}</strong></span>
      <span>Backoff <strong>{queue.delayed}</strong></span>
      <span>~{vendor.throughputPerS.toFixed(2)} files/s</span>
    </div>
  );
}
