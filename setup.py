from setuptools import setup

VERSION = "0.2"
ISRELEASED = False
__version__ = VERSION

setup(
    name = 'mongo-task',
    author = 'Robert T. McGibbon',
    version = __version__,
    license = 'MIT',
    url = 'https://github.com/rmcgibbo/mongo-task',
    entry_points = {'console_scripts':
              ['mongo-task = mongo_task:main']},
    packages= ['mongo_task'])
