#!/bin/bash

set -exv

alembic --config mergify_engine/models/db_migration/alembic.ini upgrade head
alembic --config mergify_engine/models/db_migration/alembic.ini history
alembic --config mergify_engine/models/db_migration/alembic.ini revision "$@"
