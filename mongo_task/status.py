from __future__ import print_function, absolute_import
import argparse
from bson.json_util import loads, dumps
from pprint import pprint

from .env import setup_secure_env
from .services import connect_mongo

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-e', '--env', help='Path to environment variables '
                        'file. (default .env)', default='.env')
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--skip', type=int, default=0)
    parser.add_argument('query', type=loads)
    args = parser.parse_args()

    setup_secure_env(args.env)
    cursor = connect_mongo()

    q = cursor.find(args.query).skip(args.skip).limit(args.limit)
    matches = list(iter(q))
    
    print(dumps(matches, indent=2))
    

