from invoke import task

@task
def lint(c):
    c.run("black src; flake8 src --ignore=W503,E501; isort src -m 3 --tc")
