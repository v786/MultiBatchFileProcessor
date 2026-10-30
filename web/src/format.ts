export function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 ** 2).toFixed(1)} MB`;
}

export function clock(iso: string | null | undefined): string {
  if (!iso) return '';
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function secondsAgo(iso: string | null | undefined, now = Date.now()): number {
  return iso ? Math.max(0, Math.round((now - new Date(iso).getTime()) / 1000)) : 0;
}

export const STATUS_TONE: Record<string, 'ok' | 'bad' | 'busy' | 'idle'> = {
  DONE: 'ok', COMPLETED: 'ok',
  REJECTED: 'bad', UPLOAD_EXPIRED: 'bad', EXTRACT_FAILED: 'bad', DELIVERY_FAILED: 'bad', FAILED: 'bad', PARTIAL: 'bad',
  PENDING_UPLOAD: 'idle', OPEN: 'idle',
};

export function tone(status: string) {
  return STATUS_TONE[status] ?? 'busy';
}

export function label(status: string): string {
  return status.replace(/_/g, ' ').toLowerCase();
}
