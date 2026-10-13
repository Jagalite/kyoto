import json
import plistlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from disk_io import parse_ioreg, disk_rates, sample_disks, path_device, job_devices, diagnose, volume_devices


def counters(t, ops=10, nanos=20000000, identity=1):
    return dict(valid=True, monotonic=t, devices={'disk6': dict(identity=identity,
                read_ops=ops, read_bytes=ops * 1048576, read_ns=nanos,
                write_ops=0, write_bytes=0, write_ns=0)})


class DiskIOTest(unittest.TestCase):
    def test_idle_latency_spike_is_not_disk_pressure(self):
        from disk_io import pressured_disks
        sample = disk_rates(counters(0,0,0), counters(2,1,1000000000))
        self.assertEqual(pressured_disks(sample), [])
        sample['devices']['disk6'].update(read_iops=100, io_time_per_second=2)
        self.assertEqual(pressured_disks(sample), ['disk6'])
        sample['valid'] = False
        self.assertEqual(pressured_disks(sample), [])
    def test_macos_physical_media_and_nanosecond_counters(self):
        stats = {key + ' (' + direction + ')': value for direction in ('Read', 'Write')
                 for key, value in [('Bytes', 1048576), ('Operations', 2), ('Total Time', 4000000)]}
        result = parse_ioreg([dict(IORegistryEntryID=9, Statistics=stats,
            IORegistryEntryChildren=[{'BSD Name': 'disk6', 'Whole': True,
                                     'IORegistryEntryChildren': [{'BSD Name': 'disk6s2'}]}])])
        self.assertEqual(list(result), ['disk6'])
        self.assertEqual(result['disk6']['read_ns'], 4000000)

    def test_rates_idle_reset_replacement_and_warmup(self):
        first, second = counters(1), counters(3, 14, 60000000)
        d = disk_rates(first, second)['devices']['disk6']
        self.assertEqual(d['read_iops'], 2)
        self.assertEqual(d['read_mib_s'], 2)
        self.assertEqual(d['read_ms'], 10)
        self.assertEqual(d['io_time_per_second'], .02)
        self.assertIsNone(d['write_ms'])
        self.assertEqual(disk_rates(None, first)['state'], 'warming up')
        self.assertEqual(disk_rates(second, counters(5))['devices'], {})
        self.assertEqual(disk_rates(first, counters(3, identity=2))['devices'], {})
        self.assertFalse(disk_rates(first, first)['valid'])

    def test_failure_is_explicit_and_not_an_idle_device(self):
        with patch('disk_io.sys.platform', 'darwin'), patch('disk_io.bounded_output', side_effect=OSError('missing')):
            sample = sample_disks()
        self.assertFalse(sample['valid'])
        self.assertEqual(disk_rates(None, sample)['state'], 'unavailable')

    def test_apfs_physical_store_mapping(self):
        rows = [{'IORegistryEntryChildren': [{'Whole': True, 'BSD Name': 'disk0',
                'IORegistryEntryChildren': [{'BSD Name': 'disk3s1'}]}]}]
        volumes = volume_devices(rows)
        with patch('disk_io.bounded_output', return_value='Filesystem blocks used avail capacity Mounted on\n/dev/disk3s1 1 1 0 100% /\n'), patch('disk_io.sample_disks', return_value={'volumes': volumes}):
            self.assertEqual(path_device('/')['devices'], ['disk0'])

    def test_env_wrapper_target_resolution(self):
        job = dict(id='a', cwd='/tmp', command=json.dumps(['env', 'CARGO_TARGET_DIR=/target', 'cargo', 'build']), environment='{}')
        def discover(command, cwd, env, explicit, details, probe):
            self.assertEqual(command, ['cargo', 'build'])
            self.assertEqual(env['CARGO_TARGET_DIR'], '/target')
            details['target_directory'] = '/target'
            return [], 'resolved'
        with patch('disk_io.path_device', side_effect=lambda p: {'path': p}), patch('disk_io.discover_resources', side_effect=discover):
            self.assertEqual(job_devices(job)['target']['path'], '/target')

    def test_diagnostic_does_not_create_queue_or_recover_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            host = dict(sampled_at=1, disk_counters=counters(1))
            with patch('pressure.sample_host', return_value=host), patch('disk_io.time.sleep'), patch('builtins.print'):
                diagnose(root, 1, 1)
            self.assertEqual(list(root.iterdir()), [])
