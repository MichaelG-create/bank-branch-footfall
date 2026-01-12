#!/bin/bash
source .venv/bin/activate

# Load environment variables from .env file for airflow if used locally (not needed for docker container)
set -a
source .env
set +a
