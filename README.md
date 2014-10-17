mongo-task
==========

Small wrapper for running large-scale embarrassingly parallel with MongoDB
+ Amazon S3.

```
$ mongo-task -t task.yaml
```

- You setup a MongoDB instance containing a set of documents which correspond
  to tasks. A 'status' field is used by this script to keep track of
  new / completed / failed jobs.
- When executed, this script will
   - Check out a job from the DB
   - Run it
   - Upload any declared output files to Amazon S3
   - Record the status, and any stdout/stderr in the DB


The task is declared in a simple YAML markup

```
$ cat task.yaml
output_files:
    - output.gz

task:
    - source activate py3
    - echo HELLO > output
    - gzip output
```

You also need to specify some secret environment variables for connecting
to the DB and S3. Put them in a file named `.env`

```
$ cat .env
MONGO_URL = <your.db.com:10069/dbname>
MONGO_USER = <your-mongo-username>
MONGO_PASSWORD = <your-mongo-password>
MONGO_DATABASE = <dbname>
MONGO_COLLECTION = <your-mongo-collection>
AWS_ACCESS_KEY_ID = <your-access-key>
AWS_SECRET_ACCESS_KEY = <your-secret-key>
AWS_S3_BUCKET_NAME = <your-bucket>
```


