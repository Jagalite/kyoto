"""Platform-specific termination for bounded discovery probes."""
import os
import signal
import subprocess
import uuid


def spawn_probe(command, **kwargs):
    if os.name != 'nt':
        return subprocess.Popen(command, start_new_session=True, **kwargs)
    from win32_runtime import Job
    job = Job(uuid.uuid4().hex, create=True)
    try:
        process = job.spawn(command, **kwargs)
    except BaseException:
        job.close()
        raise
    process.kyoto_job = job
    return process


def kill_probe(process):
    if os.name == 'nt':
        process.kyoto_job.terminate()
    else:
        kill_process_tree(process.pid)


def close_probe(process):
    if os.name == 'nt':
        # Probes must not leave background children behind on successful exit.
        try:
            process.kyoto_job.terminate()
        finally:
            process.kyoto_job.close()


def kill_process_tree(pid):
    if os.name == 'nt':
        from win32_runtime import terminate_tree
        terminate_tree(pid)
    else:
        os.killpg(pid, signal.SIGKILL)
