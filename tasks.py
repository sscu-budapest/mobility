from invoke import task

from src.data_locs import parts_root

@task
def lint(c):
    c.run("black src; flake8 src --ignore=W503,E501; isort src -m 3 --tc")


@task
def dvc_external_init(c):
    c.run(f"dvc add --external {parts_root}"
)