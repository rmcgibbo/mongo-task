"""
mongo-task is a small utility for running large-scale distributed embarrassingly
parallel jobs with MongoDB and Amazon S3.
"""

from __future__ import print_function, absolute_import
import os
import sys
import yaml
import argparse
import subprocess
import traceback
import hashlib

from bson.json_util import dumps
from datetime import datetime

from .env import setup_secure_env
from .utils import enter_temp_directory
from .services import upload_s3, connect_mongo


REQUIRED_ENV_VARS = {
    'MONGO_USER', 'MONGO_PASSWORD', 'MONGO_URL',
    'MONGO_DATABASE', 'MONGO_COLLECTION',
    'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_S3_BUCKET_NAME'
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-e', '--env', help='Path to environment variables '
                        'file. (default .env)', default='.env')
    parser.add_argument('-t', '--task', help='Path to task spec (YAML) file. '
                        '(default task.yaml)', default='task.yaml')
    parser.add_argument('--dry-run', action='store_true', help="Don't upload "
                        "to S3 or modify DB")
    args = parser.parse_args()

    original_env = setup_secure_env(args.env)

    for key in REQUIRED_ENV_VARS:
        if key not in os.environ:
            raise KeyError('env variable %s required' % key)

    with open(args.task) as f:
        task = yaml.load(f)
    with open(args.task) as f:
        metadata = {
            'task_sha1': hashlib.sha1(f.read()).hexdigest(),
            'command': ' '.join(sys.argv),
            'hostname': os.uname()[1]
        }

    if 'job' not in task:
        raise ValueError('task.yaml missing "job" entry')
    if 'output_files' not in task:
        task['output_files'] = []

    with enter_temp_directory():
        metadata['cwd'] = os.path.abspath(os.curdir)
        run_task(task, original_env, metadata, dry_run=args.dry_run)


def run_task(task, env, metadata, dry_run=False):
    """
    Parameters
    ----------
    task : dict
        Dict containing the parsed yaml file. Should contain
         - job : list of lines to execute
         - output_files : list of output files to upload to s3 after finishing
    env : dict
        Dict containing environment variables to be available in the
        task execution environment. Additionally, this method will set up
        'MONGOTASK_RECORD' env var, containing the job record
    metadata : dict
        Exta stuff to put in DB
    dry_run : bool, default = False
        If True, don't upload upload to the DB or push to S3
    """
    cursor = connect_mongo()

    print('Checking out new record...')
    if dry_run:
        print('metadata: ', metadata)
        record = cursor.find_one({"status": "NEW"})
    else:
        record = cursor.find_and_modify(
            query={"status": "NEW"},
            update={"$set": {"status": "PENDING",
                             "metadata": metadata,
                             "started": datetime.now()}})

    if record is None:
        print('No suitable ("status":"NEW") record found in DB')
        exit(1)

    try:
        stdout, stderr = '', ''
        env['MONGOTASK_RECORD'] = dumps(record)

        comm = subprocess.Popen(
            'sh', stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env)

        stdout, stderr = comm.communicate('\n'.join(task['job']))
        retcode = comm.poll()
        if retcode:
            raise subprocess.CalledProcessError(retcode, 'sh', output=stdout)

        results = {"status": "COMPLETE", "stdout": stdout,
                   "stderr": stderr, "retcode": retcode,
                   "completed": datetime.now()}

        if dry_run:
            print(results)
        else:
            print('Uploading results...')
            upload_s3(str(record['_id']), task['output_files'])
            cursor.find_and_modify(
                query={"_id": record['_id']},
                update={"$set": results})

    except:
        print("Job failed!")
        traceback.print_exc()

        results = {"status": "FAILED", "stdout": stdout,
                   "stderr": stderr, "retcode": retcode,
                   "completed": datetime.now()}
        if dry_run:
            print(results)
        else:
            cursor.find_and_modify(
                query={"_id": record['_id']},
                update={"$set": results})


if __name__ == '__main__':
    main()
