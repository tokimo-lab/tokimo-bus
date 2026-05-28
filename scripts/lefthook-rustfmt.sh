#!/bin/sh
# lefthook rustfmt helper — avoids inline-shell quoting issues on Windows.
# Called by lefthook.yml with {staged_files} as arguments.

for f in "$@"; do
  [ ! -f "$f" ] && continue

  dir=$(dirname "$f")
  while [ "$dir" != "." ] && [ "$dir" != "/" ] && [ ! -f "$dir/Cargo.toml" ]; do
    dir=$(dirname "$dir")
  done

  edition_line=$(grep -m1 '^edition' "$dir/Cargo.toml" 2>/dev/null)
  if echo "$edition_line" | grep -q 'workspace'; then
    # edition inherited from workspace — walk up to find workspace Cargo.toml
    ws=$(dirname "$dir")
    while [ "$ws" != "." ] && [ "$ws" != "/" ] && [ ! -f "$ws/Cargo.toml" ]; do
      ws=$(dirname "$ws")
    done
    edition=$(grep -m1 '^edition' "$ws/Cargo.toml" 2>/dev/null | sed 's/.*"\([^"]*\)".*/\1/')
  else
    edition=$(echo "$edition_line" | sed 's/.*"\([^"]*\)".*/\1/')
  fi
  rustfmt --edition "${edition:-2024}" "$f"
done
