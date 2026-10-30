// Live batch state: snapshot from GET /batches/:id, then Server-Sent Events. The stream resumes from the
// snapshot's lastEventId (and the browser resends Last-Event-ID on reconnect), so no event is missed. If SSE
// keeps failing, fall back to polling every 5 s.
import { useCallback, useEffect, useRef, useState } from 'react';
import { api, eventsUrl, type BatchView, type TaskEvent } from '../api';

export interface InAppNotice { title: string; body: string }

export function useBatch(batchId: string | null, onNotice: (n: InAppNotice) => void) {
  const [batch, setBatch] = useState<BatchView | null>(null);
  const [live, setLive] = useState<'connecting' | 'live' | 'polling'>('connecting');
  const [error, setError] = useState<string | null>(null);
  const refetchTimer = useRef<number | undefined>(undefined);
  const noticeRef = useRef(onNotice);
  noticeRef.current = onNotice;

  const refetch = useCallback(async () => {
    if (!batchId) return null;
    try {
      const b = await api.getBatch(batchId);
      setBatch(b);
      setError(null);
      return b;
    } catch (err) {
      setError(String(err));
      return null;
    }
  }, [batchId]);

  // Coalesce bursts of events into one snapshot refresh (derived text such as queue position lives server-side).
  const scheduleRefetch = useCallback(() => {
    window.clearTimeout(refetchTimer.current);
    refetchTimer.current = window.setTimeout(() => void refetch(), 400);
  }, [refetch]);

  useEffect(() => {
    if (!batchId) return;
    let source: EventSource | null = null;
    let poll: number | undefined;
    let failures = 0;
    let closed = false;
    setBatch(null);
    setLive('connecting');

    const applyEvent = (ev: TaskEvent) => {
      setBatch((b) => {
        if (!b) return b;
        const tasks = b.tasks.map((t) => (t.taskId !== ev.taskId ? t : {
          ...t,
          status: ev.to,
          progress: Math.max(t.progress, ev.progress ?? t.progress), // never goes backwards
          statusText: { ...t.statusText, text: ev.message ?? t.statusText.text, retryAt: ev.nextRetryAt },
        }));
        return { ...b, tasks, lastEventId: Math.max(b.lastEventId, ev.id) };
      });
      scheduleRefetch();
    };

    const startPolling = () => {
      setLive('polling');
      poll = window.setInterval(() => void refetch(), 5000);
    };

    void refetch().then((snap) => {
      if (closed || !snap) return;
      source = new EventSource(eventsUrl(batchId, snap.lastEventId));
      source.onopen = () => { failures = 0; setLive('live'); };
      source.addEventListener('task', (e) => applyEvent(JSON.parse((e as MessageEvent).data)));
      source.addEventListener('batch', () => void refetch());
      source.addEventListener('notification', (e) => {
        const n = JSON.parse((e as MessageEvent).data) as InAppNotice;
        noticeRef.current(n);
        if ('Notification' in window && Notification.permission === 'granted') new Notification(n.title, { body: n.body });
      });
      source.onerror = () => {
        failures++;
        setLive('connecting');
        if (failures >= 3 && source) {
          source.close();
          startPolling();
        }
      };
    });

    return () => {
      closed = true;
      source?.close();
      window.clearInterval(poll);
      window.clearTimeout(refetchTimer.current);
    };
  }, [batchId, refetch, scheduleRefetch]);

  return { batch, live, error, refetch };
}
