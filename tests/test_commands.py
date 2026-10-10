import unittest
from commands import PRIORS, classify, validate


class CommandPolicyTest(unittest.TestCase):
    def test_test_run_is_rejected(self):
        for command in (['cargo', 'test'], ['cargo', 't'], ['/bin/cargo', '+stable', '-q', 'test'],
                        ['cargo', '--config', 'build.jobs=2', 'test'],
                        ['rustup', 'run', 'stable', 'cargo', 'test'],
                        ['env', 'RUST_LOG=info', 'cargo', 'test']):
            with self.subTest(command=command):
                with self.assertRaisesRegex(ValueError, '--no-run'):
                    validate(command)

    def test_compile_only_and_help_are_allowed(self):
        self.assertEqual(validate(['cargo', 'test', '--no-run'])['kind'], 'test-compile')
        self.assertEqual(validate(['cargo', 'bench', '--no-run'])['kind'], 'bench-compile')
        self.assertEqual(validate(['cargo', 'test', '--help'])['kind'], 'metadata')
        self.assertEqual(validate(['cargo', '--help'])['kind'], 'metadata')

    def test_test_arguments_cannot_bypass_policy(self):
        for flags in (['--', '--no-run'], ['--', '--help'], ['--config', '--no-run'], ['--features', '--no-run']):
            with self.subTest(flags=flags), self.assertRaises(ValueError):
                validate(['cargo', 'test'] + flags)

    def test_bench_execution_is_rejected(self):
        with self.assertRaises(ValueError):
            validate(['cargo', 'bench'])

    def test_command_classes_have_different_starting_costs(self):
        for command, kind in [('build', 'build'), ('check', 'check'), ('clippy', 'clippy'),
                              ('fmt', 'format'), ('metadata', 'metadata'), ('doc', 'doc')]:
            self.assertEqual(classify(['cargo', command])['kind'], kind)
        self.assertLess(PRIORS['format'][0], PRIORS['check'][0])
        self.assertLess(PRIORS['check'][0], PRIORS['test-compile'][0])
        self.assertLess(PRIORS['check'][2], PRIORS['build'][2])

    def test_shell_scripts_are_explicitly_outside_argv_policy(self):
        self.assertEqual(classify(['sh', '-c', 'cargo test'])['kind'], 'opaque')
        self.assertFalse(classify(['custom-test-runner'])['executes_tests'])


if __name__ == '__main__':
    unittest.main()
