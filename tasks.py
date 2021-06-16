from invoke import task

from src.data_locs import  dvc_cache_root
from src import pipereg


for pe in pipereg.steps:
    globals()[pe.name] = pe.get_invoke_task()


@task
def lint(c):
    c.run("black src tasks.py; flake8 src --ignore=W503,E501; isort src -m 3 --tc")
