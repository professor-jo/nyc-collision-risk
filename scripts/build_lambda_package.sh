#!/usr/bin/env bash
# Rebuild lambda_package/ from the current ingestion code and pinned dependencies.
# The CDK CLI runs this before synth, diff and deploy (see "build" in infra/cdk.json).
# Set PYTHON to use a specific interpreter.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACKAGE_DIR="$ROOT/lambda_package"

# Start clean so deleted files and old dependencies can't linger
rm -rf "$PACKAGE_DIR"
mkdir "$PACKAGE_DIR"

# Install Linux wheels that match the Lambda runtime in infra_stack.py,
# whatever machine this script is running on
"${PYTHON:-python3}" -m pip install \
    --requirement "$ROOT/requirements-lambda.txt" \
    --target "$PACKAGE_DIR" \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.12 \
    --only-binary=:all: \
    --no-compile \
    --quiet

# Copy the ingestion code last, so a failed install leaves no handler to deploy
cp -R "$ROOT/ingestion" "$PACKAGE_DIR/ingestion"
rm -rf "$PACKAGE_DIR/ingestion/__pycache__"

# Console scripts aren't used by Lambda
rm -rf "$PACKAGE_DIR/bin"

echo "Built $PACKAGE_DIR"
