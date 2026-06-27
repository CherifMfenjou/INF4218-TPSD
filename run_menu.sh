#!/usr/bin/env bash
# Menu interactif du système d'apprentissage fédéré

set -e
cd "$(dirname "$0")"
source .venv/bin/activate
python -m src.menu "$@"
