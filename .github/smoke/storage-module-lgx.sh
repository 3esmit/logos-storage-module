#!/usr/bin/env bash

# Smoke test to ensure the cross-compiled .lgx carries the right payload.
# Hand-runnable: LGXDIR=result bash .github/smoke/storage-module-lgx.sh

set -euo pipefail
LGXDIR="${LGXDIR:-lgx-portable}"

lgxf=$(ls "$LGXDIR"/*.lgx) || { echo "::error::no .lgx staged under $LGXDIR"; exit 1; }
echo "asserting on $lgxf"

names=$(tar tzf "$lgxf")

grep -q '^manifest.json$' <<<"$names" || { echo "::error::no manifest.json in the archive"; exit 1; }
grep -q '^variants/windows-x86_64/' <<<"$names" || { echo "::error::no variants/windows-x86_64/ payload"; exit 1; }

if grep -qE '^variants/(linux|darwin)' <<<"$names"; then
  echo "::error::a non-Windows variant is present in a Windows-only package"
  exit 1
fi

# The plugin needs libstorage.dll in its own directory.
for want in storage_module_plugin.dll libstorage.dll; do
  grep -q "^variants/windows-x86_64/${want}$" <<<"$names" || { echo "::error::${want} missing from the payload"; exit 1; }
done

# Basecamp already provides these DLLs: the module must not bundle its own copy.
for unwanted in libstdc++-6.dll libgcc_s_seh-1.dll Qt6Core.dll libcrypto-3-x64.dll; do
  if grep -q "^variants/windows-x86_64/${unwanted}$" <<<"$names"; then
    echo "::error::${unwanted} is in the payload but the host already ships it"
    exit 1
  fi
done

echo "storage_module .lgx smoke: OK"
