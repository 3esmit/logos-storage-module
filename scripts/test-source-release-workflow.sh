#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

workflow=".github/workflows/release.yml"
action_ref="3esmit/logos-modules-release-action/.github/workflows/release.yml@81f506530c56e8757e6d99ee7f9d4c092e74411c"

assert_workflow_line() {
  local expected="$1"
  if ! grep -Fqx "$expected" "$workflow"; then
    printf 'missing release workflow contract: %s\n' "$expected" >&2
    exit 1
  fi
}

test -f "$workflow"
test "$(jq -r '.name' metadata.json)" = "storage_module"

version="$(jq -er '.version | strings | select(length > 0)' metadata.json)"
grep -Fq "## [${version}]" CHANGELOG.md

assert_workflow_line "    uses: ${action_ref}"
assert_workflow_line "      module_path: ."
assert_workflow_line "      metadata_path: metadata.json"
assert_workflow_line "      build_attr: lgx-portable"
assert_workflow_line "      variants: linux-amd64,darwin-arm64"
assert_workflow_line "      require_all_variants: true"
assert_workflow_line "      dispatch_rebuild_index: false"
assert_workflow_line "      prerelease: true"
assert_workflow_line "      signing_mode: none"

test "$(grep -Fc "$action_ref" "$workflow")" -eq 1

printf 'source release workflow contract valid for storage_module v%s\n' "$version"
