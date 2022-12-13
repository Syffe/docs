# mergify-engine

This repository contains Mergify engine code with the infrastructure to build
Docker images for customers and deploy it on Heroku in production.

## Run CI tasks locally

Install poetry and poethepoet

```
brew install poetry
poetry install
```

Get the list of CI tasks

```
$ poetry run poe
Poe the Poet - A task runner that works well with poetry.
version 0.16.5

Result: No task specified.

USAGE
  poe [-h] [-v | -q] [--root PATH] [--ansi | --no-ansi] task [task arguments]

GLOBAL OPTIONS
  ...

CONFIGURED TASKS
  linters              Run linters
  test                 Run test suite
  test-parallel        Run test suite in parallel
  record               Record test suite fixtures
  ...
```

Run a task

```
$ poetry run poe linters
```
