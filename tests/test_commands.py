import unittest
from commands import PRIORS, classify


class CommandPolicyTest(unittest.TestCase):
    def test_test_execution_is_classified(self):
        for command in (['cargo', 'test'], ['cargo', 't'], ['/bin/cargo', '+stable', '-q', 'test'],
                        ['cargo', '--config', 'build.jobs=2', 'test'],
                        ['rustup', 'run', 'stable', 'cargo', 'test'],
                        ['env', 'RUST_LOG=info', 'cargo', 'test']):
            with self.subTest(command=command):
                self.assertEqual(classify(command)['kind'], 'test-run')
                self.assertTrue(classify(command)['executes_tests'])

    def test_compile_only_and_help_are_allowed(self):
        self.assertEqual(classify(['cargo', 'test', '--no-run'])['kind'], 'test-compile')
        self.assertEqual(classify(['cargo', 'bench', '--no-run'])['kind'], 'bench-compile')
        self.assertEqual(classify(['cargo', 'test', '--help'])['kind'], 'metadata')
        self.assertEqual(classify(['cargo', '--help'])['kind'], 'metadata')

    def test_test_arguments_cannot_bypass_policy(self):
        for flags in (['--', '--no-run'], ['--', '--help'], ['--config', '--no-run'], ['--features', '--no-run']):
            with self.subTest(flags=flags):
                self.assertTrue(classify(['cargo', 'test'] + flags)['executes_tests'])

    def test_bench_execution_is_classified(self):
        self.assertEqual(classify(['cargo', 'bench'])['kind'], 'bench-run')

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
