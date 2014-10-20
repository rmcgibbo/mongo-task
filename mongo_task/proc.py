import subprocess


def execute(commands, env=None):
    if env is None:
        env = {}

    stdout = []
    stderr = []
    retcodes = []

    for command in commands:

        comm = subprocess.Popen(
            'sh', stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env)

        stdout_, stderr_ = comm.communicate(command)
        retcode = comm.poll()

        stdout.append(stdout_)
        stderr.append(stderr_)
        retcodes.append(retcode)

    return '\n'.join(stdout), '\n'.join(stderr), all(r > 0 for r in retcodes)
