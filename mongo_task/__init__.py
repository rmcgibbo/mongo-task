#!/usr/bin/env python
from __future__ import print_function
import os
import yaml
import shutil
import argparse
import subprocess
import contextlib
import tempfile
import traceback

import boto
from bson.json_util import dumps
from datetime import datetime
from boto.s3.key import Key
from pymongo import MongoClient


REQUIRED_ENV_VARS = {
    'MONGO_USER', 'MONGO_PASSWORD', 'MONGO_URL',
    'MONGO_DATABASE', 'MONGO_COLLECTION',
    'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_S3_BUCKET_NAME'
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--env', help='Environment variable',
                        default='.env')
    parser.add_argument('-t', '--task', help='Task YAML file',
                        default='task.yaml')
    args = parser.parse_args()

    original_env = setup_secure_env(args.env)

    for key in REQUIRED_ENV_VARS:
        if key not in os.environ:
            raise KeyError('env variable %s required' % key)

    with open(args.task) as f:
        task = yaml.load(f)

    with enter_temp_directory():
        run_task(task, original_env)


def run_task(task, task_env):
    assert 'task' in task
    assert 'output_files' in task
    cursor = connect_mongo()

    print('Checking out new record...')
    record = cursor.find_and_modify(
        query={"status": "NEW"},
        update={"$set": {"status": "PENDING",
                         "started": datetime.now()}})

    if record is None:
        print('No suitable ("status":"NEW") record found in DB')
        exit(1)

    try:
        stdout, stderr = '', ''
        task_env['record'] = dumps(record)

        comm = subprocess.Popen(
            'sh', stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=task_env)

        stdout, stderr = comm.communicate('\n'.join(task['task']))
        retcode = comm.poll()
        if retcode:
            raise subprocess.CalledProcessError(retcode, 'sh', output=stdout)

        print('Uploading results...')
        upload_s3(str(record['_id']), task['output_files'])
        cursor.find_and_modify(
            query={"_id": record['_id']},
            update={"$set": {"status": "COMPLETE",
                             "stdout": stdout,
                             "stderr": stderr,
                             "retcode": retcode,
                             "completed": datetime.now()}})

    except:
        print("Job failed!")
        traceback.print_exc()

        cursor.find_and_modify(
            query={"_id": record['_id']},
            update={"$set": {"status": "FAILED",
                             "stdout": stdout,
                             "stderr": stderr,
                             "retcode": retcode,
                             "completed": datetime.now()}})


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
    with open(env) as f:
        print('Loading secret environment variables from %s...' % env)
        for line in f:
            line = line.strip()
            if not line:
                continue
            key, value = [e.strip() for e in line.split('=')]
            os.environ[key] = value.strip()
    return original_env


def connect_mongo():
    uri = 'mongodb://%s:%s@%s' % (os.environ['MONGO_USER'],
                                  os.environ['MONGO_PASSWORD'],
                                  os.environ['MONGO_URL'])
    client = MongoClient(uri, safe=True)
    cursor = getattr(client, os.environ['MONGO_DATABASE'])
    return getattr(cursor, os.environ['MONGO_COLLECTION'])


def upload_s3(prefix, filenames):
    conn = boto.connect_s3(os.environ['AWS_ACCESS_KEY_ID'],
                           os.environ['AWS_SECRET_ACCESS_KEY'])
    bucket = conn.get_bucket(os.environ['AWS_S3_BUCKET_NAME'])

    for filename in filenames:
        if os.path.exists(filename):
            k = Key(bucket)
            k.key = os.path.join(prefix, filename)
            k.set_contents_from_filename(filename)
        else:
            print('%s does not exist!' % filename)


@contextlib.contextmanager
def enter_temp_directory():
    """Create and enter a temporary directory; used as context manager."""
    temp_dir = tempfile.mkdtemp()
    cwd = os.getcwd()
    os.chdir(temp_dir)
    yield
    os.chdir(cwd)
    shutil.rmtree(temp_dir)


if __name__ == '__main__':
    main()
