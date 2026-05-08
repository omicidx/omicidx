#!/usr/bin/env bash
# Reload the Dagster code location via the webserver's GraphQL API.
#
# Usage:
#   DAGSTER_URL=https://dagster.example.org ./dagster-reload.sh           # reload all locations
#   DAGSTER_URL=https://dagster.example.org ./dagster-reload.sh <name>    # reload one location
#
# Equivalent to clicking the reload button in the UI. Use after editing
# bind-mounted source so the code-server picks up the changes.

set -euo pipefail

: "${DAGSTER_URL:?Set DAGSTER_URL to the Dagster webserver root, e.g. https://dagster.example.org}"

endpoint="${DAGSTER_URL%/}/graphql"

if [[ $# -eq 0 ]]; then
    query='mutation { reloadWorkspace { __typename ... on Workspace { locationEntries { name loadStatus locationOrLoadError { __typename ... on PythonError { message } } } } ... on PythonError { message } ... on UnauthorizedError { message } } }'
else
    name="$1"
    query="mutation { reloadRepositoryLocation(repositoryLocationName: \"${name}\") { __typename ... on WorkspaceLocationEntry { name loadStatus locationOrLoadError { __typename ... on PythonError { message stack } } } ... on PythonError { message stack } ... on UnauthorizedError { message } ... on ReloadNotSupported { message } } }"
fi

curl -sSf -H 'Content-Type: application/json' \
    -d "$(jq -n --arg q "$query" '{query: $q}')" \
    "$endpoint" | jq .
