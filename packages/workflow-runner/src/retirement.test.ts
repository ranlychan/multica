// @vitest-environment node
import { afterEach, expect, it, vi } from 'vitest';
import { retireWithHeartbeat, writeRetirementHeartbeat } from './retirement.js';

afterEach(() => vi.useRealTimers());

it('recognizes only the pin-proven inactive-listener reply as a completed heartbeat write', async () => {
  await expect(writeRetirementHeartbeat(async () => { throw { code: 9 }; })).resolves.toBeUndefined();
  for (const code of [7, 16, '9', undefined]) {
    await expect(writeRetirementHeartbeat(async () => { throw { code }; })).rejects.toBeDefined();
  }
});

it('keeps one bounded heartbeat in flight while public stop waits and aborts it on drain', async () => {
  vi.useFakeTimers();
  let finish!: () => void;
  const stopped = new Promise<void>(resolve => { finish = resolve; });
  let active = 0, peak = 0;
  const signals: AbortSignal[] = [];
  const retiring = retireWithHeartbeat(() => stopped, async signal => {
    signals.push(signal); active++; peak = Math.max(peak, active);
    try { await new Promise<void>((_, reject) => signal.addEventListener('abort', () => reject(Error('aborted')), { once: true })); }
    finally { active--; }
  });
  await vi.advanceTimersByTimeAsync(12000);
  expect(signals).toHaveLength(2);
  expect(signals[0]!.aborted).toBe(true);
  expect(peak).toBe(1);
  finish(); await retiring;
  expect(signals[1]!.aborted).toBe(true);
  expect(active).toBe(0);
  await vi.advanceTimersByTimeAsync(20000);
  expect(signals).toHaveLength(2);
});

it('continues after expected status9 until stop settles, then leaves no timer', async () => {
  vi.useFakeTimers();
  let finish!: () => void;
  const stopped = new Promise<void>(resolve => { finish = resolve; });
  let writes = 0;
  const retiring = retireWithHeartbeat(() => stopped, async () => { writes++; throw { code: 9 }; });
  await vi.advanceTimersByTimeAsync(12500);
  expect(writes).toBe(4);
  finish(); await retiring;
  expect(vi.getTimerCount()).toBe(0);
  await vi.advanceTimersByTimeAsync(40000);
  expect(writes).toBe(4);
});

it('aborts continuation when public stop fails', async () => {
  vi.useFakeTimers();
  let fail!: (error: Error) => void;
  const stopped = new Promise<void>((_, reject) => { fail = reject; });
  let signal!: AbortSignal;
  const retiring = retireWithHeartbeat(() => stopped, async current => {
    signal = current;
    await new Promise<void>((_, reject) => current.addEventListener('abort', () => reject(Error('aborted')), { once: true }));
  });
  const result = expect(retiring).rejects.toThrow('stop failed');
  fail(Error('stop failed')); await result;
  expect(signal.aborted).toBe(true);
  expect(vi.getTimerCount()).toBe(0);
});
