/** At the engine pin, status9 follows the heartbeat write for an inactive listener. */
export async function writeRetirementHeartbeat(heartbeat: () => Promise<unknown>): Promise<void> {
  try { await heartbeat(); }
  catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === 9) return;
    throw error;
  }
}

/** Preserve already-running work while public stop unregisters and awaits futures.
 * Availability failures retry; they do not create authority or prevent reassignment.
 * The Go owner still owns IPC lifetime and the original credential deadline. */
export async function retireWithHeartbeat(stop: () => Promise<void>, heartbeat: (signal: AbortSignal) => Promise<unknown>): Promise<void> {
  let stopping = false;
  let abortAttempt: (() => void) | undefined;
  let wake: (() => void) | undefined;
  const keepAlive = (async () => {
    while (!stopping) {
      const attempt = new AbortController();
      abortAttempt = () => attempt.abort();
      const timeout = setTimeout(abortAttempt, 5000);
      try { await writeRetirementHeartbeat(() => heartbeat(attempt.signal)); }
      catch { /* A partition may still lose assignment; the action verifier remains strict. */ }
      finally { clearTimeout(timeout); abortAttempt = undefined; }
      if (stopping) break;
      await new Promise<void>(resolve => {
        const timer = setTimeout(resolve, 4000);
        wake = () => { clearTimeout(timer); resolve(); };
      });
      wake = undefined;
    }
  })();
  try { await stop(); }
  finally {
    stopping = true;
    abortAttempt?.(); wake?.();
    await keepAlive;
  }
}
