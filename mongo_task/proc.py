from __future__ import print_function
import subprocess


def execute(commands, env=None, dry_run=False):
    if env is None:
        env = {}

    stdout = []
    stderr = []
    retcodes = []

    for command in commands:
        if dry_run:
            print('\033[94mexec\033[0m: %s' % command)
        comm = subprocess.Popen(
            'sh', stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env)

        stdout_, stderr_ = comm.communicate(command)
        retcode = comm.poll()

        if dry_run:
            print('\033[92mstdout\033[0m: %s' % stdout_)
            print('\033[93mstderr\033[0m: %s' % stderr_)
            print('\033[91mretcode\033[0m: %s' % retcode)

        stdout.append(stdout_)
        stderr.append(stderr_)
        retcodes.append(retcode)

    success = all(r == 0 for r in retcodes)
    return '\n'.join(stdout), '\n'.join(stderr), success
