#!/bin/bash
# ============================================================
# Run this from your project root to create all required
# directories and placeholder files for Phase 1.
#
# Usage:  bash scripts/setup_directories.sh
# ============================================================

echo "Creating project directory structure..."

# Core source packages
mkdir -p src/ingestion src/quality src/models src/database src/utils

# Create all __init__.py files
touch src/__init__.py \
      src/ingestion/__init__.py \
      src/quality/__init__.py \
      src/models/__init__.py \
      src/database/__init__.py \
      src/utils/__init__.py

# Test directories
mkdir -p tests/unit tests/integration
touch tests/__init__.py tests/unit/__init__.py tests/integration/__init__.py

# DAGs directory (Phase 2 — placeholder for now)
mkdir -p dags
touch dags/.gitkeep

# Great Expectations (Phase 4)
mkdir -p great_expectations/expectations great_expectations/checkpoints
mkdir -p great_expectations/uncommitted/validations
mkdir -p great_expectations/uncommitted/data_docs/local_site
touch great_expectations/expectations/.gitkeep
touch great_expectations/checkpoints/.gitkeep
touch great_expectations/uncommitted/.gitkeep

# Streamlit app (Phase 5 — placeholder for now)
mkdir -p streamlit_app/pages streamlit_app/components
touch streamlit_app/.gitkeep

# Scripts directory
mkdir -p scripts

# Alembic migrations
mkdir -p src/database/migrations/versions

echo "✓ All directories created!"
echo ""
echo "Directory structure:"
find . -type f -name "*.py" -o -name "*.gitkeep" -o -name "*.yml" -o -name "*.sql" \
    -o -name "*.txt" -o -name "*.sh" -o -name "Dockerfile*" -o -name ".env*" \
    | grep -v __pycache__ | grep -v .git/ | sort