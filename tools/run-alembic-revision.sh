#!/bin/bash

set -exv

alembic upgrade head
alembic history
alembic revision "$@"
