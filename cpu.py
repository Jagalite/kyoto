"""Host-wide CPU tick counters; utilization is measured between samples."""
import ctypes
from pathlib import Path
import sys


def sample_ticks():
    if sys.platform == 'win32':
        import psutil
        times = psutil.cpu_times()
        return [times.user, times.system, times.idle], [2]
    if sys.platform == 'darwin':
        lib = ctypes.CDLL('/usr/lib/libSystem.B.dylib')
        lib.mach_host_self.restype = ctypes.c_uint
        lib.host_statistics.argtypes = [ctypes.c_uint, ctypes.c_int, ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_uint)]
        lib.mach_port_deallocate.argtypes = [ctypes.c_uint, ctypes.c_uint]
        host = lib.mach_host_self()
        ticks = (ctypes.c_uint * 4)()
        count = ctypes.c_uint(4)
        try:
            code = lib.host_statistics(host, 3, ctypes.cast(ticks, ctypes.POINTER(ctypes.c_int)), ctypes.byref(count))
            if code != 0 or count.value != 4:
                raise OSError('CPU statistics unavailable')
            # Mach CPU_STATE_USER, SYSTEM, IDLE, NICE.
            return list(ticks), [2]
        finally:
            lib.mach_port_deallocate(ctypes.c_uint.in_dll(lib, 'mach_task_self_').value, host)
    values = [int(value) for value in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
    if len(values) < 4:
        raise ValueError('CPU statistics unavailable')
    # Linux guest time is already included in user/nice; exclude guest columns.
    return values, [3, 4] if len(values) > 4 else [3]


def utilization(previous, current, idle_indices):
    if previous is None or len(previous) != len(current):
        return None
    differences = [new - old for old, new in zip(previous, current)]
    total = sum(differences)
    if total <= 0 or any(value < 0 for value in differences):
        return None
    idle = sum(differences[index] for index in idle_indices)
    return round(100 * (total - idle) / total, 1)
