import os


def setup_secure_env(env='.env'):
    """
    Load (secret) environment variables from a file into current
    environment

    Returns
    --------
    original_env : dict
        The original environment
    """
    original_env = os.environ.copy()
    if os.path.exists(env):
        with open(env) as f:
            print('Loading secret environment variables from %s...' % env)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                key, value = [e.strip() for e in line.split('=')]
                os.environ[key] = value.strip()
    return original_env
