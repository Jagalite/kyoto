"""Windows locks, durable named Job Objects, and host measurements.

An independent guardian retains each job handle while commands survive, even
if their runner or scheduler exits. No kill-on-close policy is installed.
"""
import ctypes
from ctypes import wintypes as W
import errno
import msvcrt
import subprocess
import time

try:
    import psutil
except ImportError as exc:
    raise ImportError('Windows support requires: python -m pip install -r requirements-windows.txt') from exc

k32 = ctypes.WinDLL('kernel32', use_last_error=True)
for name, args, result in (
    ('CreateJobObjectW', [ctypes.c_void_p, W.LPCWSTR], W.HANDLE),
    ('OpenJobObjectW', [W.DWORD, W.BOOL, W.LPCWSTR], W.HANDLE),
    ('CloseHandle', [W.HANDLE], W.BOOL),
    ('AssignProcessToJobObject', [W.HANDLE, W.HANDLE], W.BOOL),
    ('TerminateJobObject', [W.HANDLE, W.UINT], W.BOOL),
    ('QueryInformationJobObject', [W.HANDLE, ctypes.c_int, ctypes.c_void_p, W.DWORD, ctypes.c_void_p], W.BOOL),
):
    function = getattr(k32, name)
    function.argtypes, function.restype = args, result


def check(value):
    if not value:
        raise ctypes.WinError(ctypes.get_last_error())
    return value


class Locks:
    LOCK_EX = 2
    LOCK_NB = 4

    @staticmethod
    def flock(stream, flags):
        stream.seek(0)
        while True:
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                    raise
                if flags & Locks.LOCK_NB:
                    raise BlockingIOError(exc.errno, str(exc)) from exc
                time.sleep(.05)


fcntl = Locks()


class Job:
    def __init__(self, token, create=False):
        name = 'Local\\Kyoto-' + token
        self.handle = (check(k32.CreateJobObjectW(None, name)) if create else
                       k32.OpenJobObjectW(0x1f003f, False, name))
        if not self.handle and ctypes.get_last_error() != 2:
            check(self.handle)

    def close(self):
        if self.handle:
            k32.CloseHandle(self.handle)
            self.handle = None

    def pids(self):
        if not self.handle:
            return []
        capacity = 64
        while True:
            class List(ctypes.Structure):
                _fields_ = [('assigned', W.DWORD), ('count', W.DWORD),
                            ('ids', ctypes.c_size_t * capacity)]
            value = List()
            if k32.QueryInformationJobObject(self.handle, 3, ctypes.byref(value), ctypes.sizeof(value), None):
                return list(value.ids[:value.count])
            if ctypes.get_last_error() != 234:
                check(False)
            capacity = max(capacity * 2, value.assigned)

    def terminate(self):
        if self.handle:
            check(k32.TerminateJobObject(self.handle, 1))

    def usage(self):
        class Accounting(ctypes.Structure):
            _fields_ = [('user', ctypes.c_longlong), ('kernel', ctypes.c_longlong),
                        ('period_user', ctypes.c_longlong), ('period_kernel', ctypes.c_longlong),
                        ('faults', W.DWORD), ('total', W.DWORD), ('active', W.DWORD), ('terminated', W.DWORD)]
        value = Accounting()
        check(k32.QueryInformationJobObject(self.handle, 1, ctypes.byref(value), ctypes.sizeof(value), None))
        # RSS and Unix block counts have no equivalent in basic job accounting.
        return dict(ru_utime=value.user / 10000000, ru_stime=value.kernel / 10000000,
                    ru_maxrss=None, ru_inblock=None, ru_oublock=None)

    def spawn(self, command, **kwargs):
        process = subprocess.Popen(command, close_fds=True,
                                   creationflags=0x4 | subprocess.CREATE_NO_WINDOW, **kwargs)
        try:
            check(k32.AssignProcessToJobObject(self.handle, int(process._handle)))
            resume = ctypes.WinDLL('ntdll').NtResumeProcess
            resume.argtypes, resume.restype = [W.HANDLE], W.LONG
            status = resume(int(process._handle))
            if status != 0:
                raise OSError('Cannot resume Windows job process: ' + str(status))
        except BaseException:
            process.kill()
            process.wait()
            raise
        return process


def identity(pid):
    try:
        return str(psutil.Process(pid).create_time())
    except psutil.NoSuchProcess:
        return None
    except psutil.AccessDenied as exc:
        raise PermissionError(str(exc)) from exc


def memory():
    value = psutil.virtual_memory()
    # Windows does not expose Unix swap-in/out counters through psutil.
    return dict(available_mb=value.available / 1048576, total_mb=value.total / 1048576,
                swap_used_mb=None, swap_telemetry_supported=False, pressure='normal' if value.percent < 90 else 'warning')


def terminate_tree(pid):
    try:
        process = psutil.Process(pid)
        children = process.children(recursive=True)
        for child in reversed(children):
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        process.kill()
    except psutil.NoSuchProcess:
        pass
