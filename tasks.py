from invoke import task

from src.data_locs import parts_root, dvc_cache_root


@task
def lint(c):
    c.run("black src tasks.py; flake8 src --ignore=W503,E501; isort src -m 3 --tc")


@task
def dvc_external_init(c):
    dvc_cache_root.mkdir(exist_ok=True)
    c.run(f"dvc cache dir {dvc_cache_root}")
    c.run(f"dvc add --external {parts_root}")
