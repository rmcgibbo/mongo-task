from __future__ import print_function

import os
import sys
import time
import boto
from boto.s3.key import Key
from pymongo import MongoClient
from pymongo.cursor import Cursor
from pymongo.connection import Connection
from pymongo.errors import AutoReconnect


# Monkey-patch PyMongo to avoid throwing AutoReconnect
# errors. We try to reconnect a couple times before giving up.
def reconnect(f):
    # https://gist.github.com/aliang/1393029
    def f_retry(*args, **kwargs):
        N_RECONNECT_TRIALS = 3
        for i in range(N_RECONNECT_TRIALS):
            try:
                return f(*args, **kwargs)
            except AutoReconnect as e:
                print('Fail to execute %s [%s] (attempt %d/%d)' % (
                    f.__name__, e, i, N_RECONNECT_TRIALS),
                    file=sys.stderr)
            time.sleep(1)
        raise RuntimeError('AutoReconnect failed. Fail to '
                           'execute %s [%s]' % (f.__name__, e))
    return f_retry


Cursor._Cursor__send_message = reconnect(Cursor._Cursor__send_message)
Connection._send_message = reconnect(Connection._send_message)
Connection._send_message_with_response = reconnect(Connection._send_message_with_response)
Connection._Connection__find_master = reconnect(Connection._Connection__find_master)


def connect_mongo():
    uri = 'mongodb://%s:%s@%s' % (os.environ['MONGO_USER'],
                                  os.environ['MONGO_PASSWORD'],
                                  os.environ['MONGO_URL'])
    client = MongoClient(uri, safe=True)
    cursor = getattr(client, os.environ['MONGO_DATABASE'])
    collection = getattr(cursor, os.environ['MONGO_COLLECTION'])
    return collection


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
