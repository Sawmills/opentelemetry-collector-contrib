#!/usr/bin/env bash

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

#
# verifies if the collector components are using the main core collector version
# as a dependency.
#

source ./internal/buildscripts/modules

set -eu -o pipefail

EXCEPTIONS_FILE="./.github/workflows/scripts/collector-module-version-exceptions.txt"
# "<module directory> <collector module>" pairs; awk succeeds when the list is empty.
exemptions=$(awk '!/^#/ && NF {print $1 " " $2}' "$EXCEPTIONS_FILE")

mod_files=$(find . -type f -name "go.mod")

# Check if GNU sed is installed
GNU_SED_INSTALLED=false
if sed --version 2>/dev/null | grep -q "GNU sed"; then
   GNU_SED_INSTALLED=true
fi

# Compare the collector main core version against all the collector component
# modules to verify that they are using this version as its dependency.
# Exempt (module directory, collector module) pairs keep their version.
check_collector_versions_correct() {
   collector_module="$1"
   collector_mod_version="$2"
   echo "Checking $collector_module is used with $collector_mod_version"

   skipped_files=$(printf '%s\n' "$exemptions" | awk -v m="$collector_module" '$2 == m {printf "./%s/go.mod ", $1}')
   # shellcheck disable=SC2086
   checked_files=$(printf '%s\n' $mod_files | awk -v skip="$skipped_files" '
      BEGIN {n = split(skip, list, " "); for (i = 1; i <= n; i++) if (list[i] != "") skipped[list[i]] = 1}
      !($0 in skipped)')

   # Loop through all the module files, checking the collector version
   if [ "${GNU_SED_INSTALLED}" = false ]; then
      sed -i '' "s|$collector_module [^ ]*|$collector_module $collector_mod_version|g" $checked_files
   else
      sed -i'' "s|$collector_module [^ ]*|$collector_module $collector_mod_version|g" $checked_files
   fi
}

# The fork pins one collector core for all modules. cmd/otelcontribcol aggregates the
# exempt modules, so it is not a reliable reference. Use the most common version
# across all modules instead, which is the fork-wide pin.
most_common_version() {
   collector_module="$1"
   # shellcheck disable=SC2086
   awk -v m="$collector_module" '$1 == m {print $2}' $mod_files | sort | uniq -c | sort -rn | awk 'NR == 1 {print $2}'
}

in_list() {
   needle="$1"
   shift
   for item in "$@"; do
      [ "$item" = "$needle" ] && return 0
   done
   return 1
}

module_version() {
   awk -v m="$2" '$1 == m {print $2; exit}' "$1"
}

BETA_MODULE="go.opentelemetry.io/collector"
# Note space at end of string. This is so it filters for the exact string
# only and does not return string which contains this string as a substring.
BETA_MOD_VERSION=$(most_common_version "$BETA_MODULE")
STABLE_MODULE="go.opentelemetry.io/collector/pdata"
STABLE_MOD_VERSION=$(most_common_version "$STABLE_MODULE")

# Every exemption must still be needed: the exempt requirement must exist and differ
# from the pin of its module list.
stale_exemptions=""
while read -r exempt_dir exempt_module; do
   [ -n "$exempt_dir" ] || continue
   if [ "$exempt_module" = "$STABLE_MODULE" ] || in_list "$exempt_module" "${stable_modules[@]}"; then
      pin="$STABLE_MOD_VERSION"
   elif [ "$exempt_module" = "$BETA_MODULE" ] || in_list "$exempt_module" "${beta_modules[@]}"; then
      pin="$BETA_MOD_VERSION"
   else
      echo "Error: $exempt_module in $EXCEPTIONS_FILE is not a managed collector module"
      exit 1
   fi
   current=$(module_version "./$exempt_dir/go.mod" "$exempt_module")
   if [ -z "$current" ] || [ "$current" = "$pin" ]; then
      stale_exemptions="$stale_exemptions $exempt_dir:$exempt_module"
   fi
done <<< "$exemptions"
if [ -n "$stale_exemptions" ]; then
   echo "Error: these exemptions in $EXCEPTIONS_FILE are no longer needed:$stale_exemptions"
   exit 1
fi

check_collector_versions_correct "$BETA_MODULE" "$BETA_MOD_VERSION"
for mod in "${beta_modules[@]}"; do
   check_collector_versions_correct "$mod" "$BETA_MOD_VERSION"
done

# Check stable modules
check_collector_versions_correct "$STABLE_MODULE" "$STABLE_MOD_VERSION"
for mod in "${stable_modules[@]}"; do
   check_collector_versions_correct "$mod" "$STABLE_MOD_VERSION"
done

git diff --exit-code
