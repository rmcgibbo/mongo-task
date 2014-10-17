import os
import boto
from boto.s3.key import Key
from pymongo import MongoClient


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
