"""
mongo-task is a small utility for running large-scale distributed embarrassingly
parallel jobs with MongoDB and Amazon S3.
"""

from __future__ import print_function, absolute_import
import os
import sys
import yaml
import shutil
import argparse
import hashlib
import warnings

from bson.json_util import dumps, loads
from datetime import datetime

from .env import setup_secure_env
from .utils import enter_temp_directory
from .services import upload_s3, connect_mongo
from .proc import execute


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
    parser.add_argument('--loop', action='store_true', help='Loop the entire '
                        'process untill no more jobs left in DB')
    debug_group = parser.add_argument_group('debugging arguments')
    debug_group.add_argument('--dry-run', action='store_true', help="Don't "
                             "upload to S3 or modify DB")
    debug_group.add_argument('--spoof-record', default=None, help='Spoof a '
                             'record, instead of downloading from Mongo. '
                             'Implies --dry-run. ', type=loads)
    debug_group.add_argument('--tar-all-out', default=None, help='Tar up '
                             'the entire work directory, and copy it '
                             'to the current directory, after the job '
                             'finishes.', action='store_true')
    args = parser.parse_args()

    # set up env variables from --env
    original_env = setup_secure_env(args.env)
    for key in REQUIRED_ENV_VARS:
        if key not in os.environ:
            warnings.warn('env variable %s required' % key)

    # connect to mongodb, or "spoof" it
    if args.spoof_record is None:
        cursor = connect_mongo()
    else:
        args.dry_run = True
        cursor = argparse.Namespace(find_one=lambda x: args.spoof_record)

    # read the task.yaml file and set up metadata for the task that
    # will be synced to the DB as the job is pending
    with open(args.task) as f:
        task = yaml.load(f)
    with open(args.task) as f:
        metadata = {
            'task_sha1': hashlib.sha1(f.read()).hexdigest(),
            'command': ' '.join(sys.argv),
            'hostname': os.uname()[1]
        }

    # get the key entries out of the task file
    if 'job' not in task:
        raise ValueError('task.yaml missing "job" entry')
    if 'output_files' not in task:
        task['output_files'] = []

    # name of the directory job was submitted from
    submit_dir = os.path.abspath(os.curdir)
    sucesses = []

    while True:
        # run the task inside a temp dir
        with enter_temp_directory():
            metadata['cwd'] = os.path.abspath(os.curdir)
            success = run_task(task, original_env, metadata, cursor, dry_run=args.dry_run)
            sucesses.append(success)

            # optionally tar up and copy out the tempdir
            if args.tar_all_out:
                dirname = os.path.basename(metadata['cwd'])
                execute(['cd ..; tar czf {dirname}.tgz {dirname}'.format(dirname=dirname)])
                shutil.move('../{dirname}.tgz'.format(dirname=dirname), submit_dir)

        if len(sucesses) >= 3 and all(not s for s in sucesses[-3:]):
            raise RuntimeError('Three consecutive job failires!')

        if not args.loop:
            break


def run_task(task, env, metadata, cursor, dry_run=False):
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
        Extra stuff to put in DB
    cursor : pymongo.collection.Collection
        Cursor for pymongo
    dry_run : bool, default = False
        If True, don't upload upload to the DB or push to S3

    Returns
    -------
    success : bool
        Whether the job succeeded.
    """

    print('Checking out new record...')
    checkout_time = datetime.now()
    if dry_run:
        print('DRY RUN. METADATA:\n', metadata)
        record = cursor.find_one({"status": "NEW"})
        assert isinstance(record, dict)
        print('DRY RUN RECORD:\n', record)
    else:
        record = cursor.find_and_modify(
            query={"status": "NEW"},
            update={"$set": {"status": "PENDING",
                             "metadata": metadata,
                             "started": checkout_time}})

    if record is None:
        print('No suitable ("status":"NEW") record found in DB')
        exit(1)

    env['MONGOTASK_RECORD'] = dumps(record)
    stdout, stderr, success = execute(task['job'], env, dry_run)
    results = {
        "status": "COMPLETED" if success else "FAILED",
        "stdout": stdout,
        "stderr": stderr,
        'elapsed': (datetime.now() - checkout_time).total_seconds(),
        "completed": datetime.now()}

    if dry_run:
        print('DRY RUN STATUS:\n', results)
    elif success:
        print('Success. Uploading results...')
        upload_s3(str(record['_id']), task['output_files'])
        cursor.find_and_modify(
            query={"_id": record['_id']},
            update={"$set": results})
    else:
        # failure
        print('Job failure!\n', results)
        cursor.find_and_modify(
            query={"_id": record['_id']},
            update={"$set": results})

    return success
