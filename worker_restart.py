"""Session-scoped cooperative worker restart; never signals build processes."""
import time
import uuid


def restart_worker(root, timeout):
    from kyoto import connect, ensure_worker, runtime_for, worker_ready
    db = connect(root)
    request_id = None
    try:
        previous = runtime_for(db)
        if not previous or not worker_ready(root, previous):
            raise RuntimeError('No ready worker to restart. Run kyoto.py start first.')
        if not previous.get('supports_restart'):
            raise RuntimeError('This worker predates graceful restart. The command is available after its next restart; no processes were signalled.')
        session = previous['worker_session']
        request_id = uuid.uuid4().hex
        deadline = time.monotonic() + timeout
        db.execute('DELETE FROM worker_restart WHERE expires < ?', (time.time(),))
        db.execute('INSERT INTO worker_restart(request_id,session,expires) VALUES (?,?,?)',
                   (request_id, session, time.time() + timeout))
        db.commit()
        while time.monotonic() < deadline:
            # A dashboard can win the handoff race. Any ready replacement session
            # is sufficient; session identity also handles re-exec with the same PID.
            ensure_worker(root)
            current = runtime_for(db)
            if current and current.get('worker_session') != session and worker_ready(root, current):
                return dict(restarted=True, previous_session=session, worker_session=current['worker_session'],
                            worker_pid=current['worker_pid'])
            time.sleep(.1)
        raise TimeoutError('Worker restart was not verified before timeout. Check health before retrying; running builds were not signalled.')
    finally:
        if request_id is not None:
            db.execute('DELETE FROM worker_restart WHERE request_id=?', (request_id,))
            db.commit()
        db.close()
