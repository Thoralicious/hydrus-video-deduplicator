#!/usr/bin/env bash

set -euo pipefail

command=(python -m hydrusvideodeduplicator)

if [[ -n "${HYDRUS_API_KEY:-}" ]]; then command+=(--api-key "$HYDRUS_API_KEY"); fi
if [[ -n "${HYDRUS_API_URL:-}" ]]; then command+=(--api-url "$HYDRUS_API_URL"); fi
if [[ -n "${SIMILARITY_THRESHOLD:-}" ]]; then command+=(--threshold "$SIMILARITY_THRESHOLD"); fi
if [[ -n "${JOB_COUNT:-}" ]]; then command+=(--job-count "$JOB_COUNT"); fi
if [[ -n "${FAILED_PAGE_NAME:-}" ]]; then command+=(--failed-page-name "$FAILED_PAGE_NAME"); fi
if [[ -n "${DEDUP_DATABASE_DIR:-}" ]]; then command+=(--dedup-database-dir "$DEDUP_DATABASE_DIR"); fi

if [[ "${SKIP_HASHING:-false}" == "true" ]]; then
    command+=(--skip-hashing)
else
    command+=(--no-skip-hashing)
fi
if [[ "${CLEAR_SEARCH_TREE:-false}" == "true" ]]; then
    command+=(--clear-search-tree)
else
    command+=(--no-clear-search-tree)
fi
if [[ "${CLEAR_SEARCH_CACHE:-false}" == "true" ]]; then
    command+=(--clear-search-cache)
else
    command+=(--no-clear-search-cache)
fi
if [[ "${VERBOSE:-false}" == "true" ]]; then
    command+=(--verbose)
else
    command+=(--no-verbose)
fi
if [[ "${DEBUG:-false}" == "true" ]]; then
    command+=(--debug)
else
    command+=(--no-debug)
fi

exec "${command[@]}"
