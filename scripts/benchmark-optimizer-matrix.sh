#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 OUTPUT-DIRECTORY" >&2
  exit 2
fi

output_dir=$1
build_dir=${NATS_BENCH_BUILD_DIR:-_build-bench}
wait_quiet=${NATS_BENCH_WAIT_QUIET_SECONDS:-120}
allow_loaded=${NATS_BENCH_ALLOW_LOADED:-0}
summarize_only=${NATS_BENCH_SUMMARIZE_ONLY:-0}

case "$allow_loaded:$summarize_only" in
  0:0|0:1|1:0|1:1) ;;
  *)
    echo "NATS_BENCH_ALLOW_LOADED and NATS_BENCH_SUMMARIZE_ONLY must be 0 or 1" >&2
    exit 2
    ;;
esac

if ! command -v dune >/dev/null 2>&1 || ! command -v ocamlopt >/dev/null 2>&1; then
  echo "dune and ocamlopt must come from the project Nix shell" >&2
  echo "run: nix develop .#test -c $0 OUTPUT-DIRECTORY" >&2
  exit 2
fi
if [ "$(dune --version)" != 3.24.2 ] || [ "$(ocamlopt -version)" != 5.5.0 ] ||
  [ "$(ocamlopt -config-var flambda)" != true ]; then
  echo "benchmarking requires Dune 3.24.2 and the Flambda-enabled OCaml 5.5.0 project toolchain" >&2
  echo "run: nix develop .#test -c $0 OUTPUT-DIRECTORY" >&2
  exit 2
fi

directory_is_empty() {
  for entry in "$1"/* "$1"/.[!.]* "$1"/..?*; do
    if [ -e "$entry" ] || [ -L "$entry" ]; then
      return 1
    fi
  done
  return 0
}

if [ "$summarize_only" -eq 1 ]; then
  if [ ! -d "$output_dir" ]; then
    echo "results directory does not exist: $output_dir" >&2
    exit 2
  fi
elif [ -e "$output_dir" ]; then
  if [ ! -d "$output_dir" ] || ! directory_is_empty "$output_dir"; then
    echo "output directory must be absent or empty: $output_dir" >&2
    exit 2
  fi
else
  mkdir -p "$output_dir"
fi

run_profile() {
  profile=$1
  baseline=$output_dir/$profile.thumper
  json=$output_dir/$profile.json
  report=$output_dir/$profile.txt

  echo "measuring $profile" >&2
  if dune exec \
    --build-dir "$build_dir" \
    --workspace dune-workspace.bench \
    --profile "$profile" \
    bench/bench_protocol.exe -- \
    check \
    --color never \
    --wait-quiet "$wait_quiet" \
    --baseline "$baseline" \
    --json "$json" >"$report"
  then
    status=0
  else
    status=$?
  fi

  cat "$report"
  if [ "$status" -ne 0 ]; then
    return "$status"
  fi
  if grep -Fq 'measured under load' "$report"; then
    if [ "$allow_loaded" -eq 0 ]; then
      echo "refusing to record loaded-host timings for $profile" >&2
      return 1
    fi
    echo "warning: retaining loaded-host timings for exploratory reporting" >&2
  fi
  if [ ! -f "$baseline.corrected" ]; then
    echo "benchmark did not produce a baseline: $baseline.corrected" >&2
    return 1
  fi
  mv "$baseline.corrected" "$baseline"
}

if [ "$summarize_only" -eq 0 ]; then
  run_profile bench_no_opt
  run_profile bench_o3
  run_profile bench_o3_unbox
fi

for profile in bench_no_opt bench_o3 bench_o3_unbox; do
  for extension in thumper txt; do
    result=$output_dir/$profile.$extension
    if [ ! -f "$result" ]; then
      echo "missing benchmark result: $result" >&2
      exit 2
    fi
  done
done

summary=$output_dir/summary.md
summary_tmp=$output_dir/.summary.md.tmp.$$
trap 'rm -f "$summary_tmp"' EXIT HUP INT TERM
generated_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
ocaml_version=$(ocamlopt -version)
dune_version=$(dune --version)
flambda=$(ocamlopt -config-var flambda)
platform=$(uname -srm)

{
  printf '# NATS protocol optimizer benchmark\n\n'
  printf '%s\n' "- Generated: $generated_at"
  printf '%s\n' "- Platform: $platform"
  printf '%s\n' "- OCaml: $ocaml_version"
  printf '%s\n' "- Dune: $dune_version"
  printf '%s\n\n' "- Flambda: $flambda"
  if grep -Fq 'measured under load' "$output_dir"/*.txt; then
    printf '**Caution:** At least one profile was measured under load. Treat timing deltas as exploratory, not as a regression baseline.\n\n'
  fi
  printf '## Wall time\n\n'
  printf '| Case | Oclassic | O3 | O3 vs Oclassic | O3 + closure unboxing | Unboxing vs Oclassic | Unboxing vs O3 |\n'
  printf '|---|---:|---:|---:|---:|---:|---:|\n'
} >"$summary_tmp"

awk -F '\t' '
  function time(value) {
    if (value < 0.000001) return sprintf("%.2f ns", value * 1000000000)
    if (value < 0.001) return sprintf("%.2f us", value * 1000000)
    return sprintf("%.2f ms", value * 1000)
  }
  function delta(value, baseline) {
    return sprintf("%+.1f%%", ((value / baseline) - 1) * 100)
  }
  FNR == 1 && $0 != "# thumper baseline v1" {
    print "unsupported Thumper baseline format in " FILENAME > "/dev/stderr"
    invalid_format = 1
  }
  FILENAME == ARGV[1] && $2 == "wall_time" {
    order[++count] = $1
    no_opt[$1] = $5
    no_opt_wall[$1] = 1
  }
  FILENAME == ARGV[1] && $2 == "alloc_words" {
    no_opt_alloc[$1] = $4
    no_opt_allocation[$1] = 1
  }
  FILENAME == ARGV[2] && $2 == "wall_time" {
    o3[$1] = $5
    o3_wall[$1] = 1
    o3_count++
  }
  FILENAME == ARGV[2] && $2 == "alloc_words" {
    o3_alloc[$1] = $4
    o3_allocation[$1] = 1
  }
  FILENAME == ARGV[3] && $2 == "wall_time" {
    unbox[$1] = $5
    unbox_wall[$1] = 1
    unbox_count++
  }
  FILENAME == ARGV[3] && $2 == "alloc_words" {
    unbox_alloc[$1] = $4
    unbox_allocation[$1] = 1
  }
  END {
    if (invalid_format) exit 2
    if (count != o3_count || count != unbox_count) {
      print "benchmark profiles contain different case counts" > "/dev/stderr"
      exit 2
    }
    for (row = 1; row <= count; row++) {
      case_name = order[row]
      if (!(case_name in no_opt_wall) || !(case_name in o3_wall) || \
          !(case_name in unbox_wall) || !(case_name in no_opt_allocation) || \
          !(case_name in o3_allocation) || !(case_name in unbox_allocation)) {
        printf "benchmark case is incomplete across profiles: %s\n", \
          case_name > "/dev/stderr"
        incomplete = 1
      }
      if (no_opt[case_name] <= 0 || o3[case_name] <= 0 || \
          unbox[case_name] <= 0) {
        printf "benchmark case has a non-positive wall time: %s\n", \
          case_name > "/dev/stderr"
        incomplete = 1
      }
    }
    if (incomplete) exit 2
    for (row = 1; row <= count; row++) {
      case_name = order[row]
      printf "| `%s` | %s | %s | %s | %s | %s | %s |\n", \
        case_name, time(no_opt[case_name]), time(o3[case_name]), \
        delta(o3[case_name], no_opt[case_name]), time(unbox[case_name]), \
        delta(unbox[case_name], no_opt[case_name]), \
        delta(unbox[case_name], o3[case_name])
    }
    printf "\n## Allocation\n\n"
    printf "| Case | Oclassic | O3 | O3 + closure unboxing |\n"
    printf "|---|---:|---:|---:|\n"
    for (row = 1; row <= count; row++) {
      case_name = order[row]
      printf "| `%s` | %s words | %s words | %s words |\n", \
        case_name, no_opt_alloc[case_name], o3_alloc[case_name], \
        unbox_alloc[case_name]
    }
  }
' \
  "$output_dir/bench_no_opt.thumper" \
  "$output_dir/bench_o3.thumper" \
  "$output_dir/bench_o3_unbox.thumper" >>"$summary_tmp"

printf '\nNegative percentages are faster. Raw Thumper reports, JSON verdicts, and baselines are in %s.\n' \
  "$output_dir" >>"$summary_tmp"

mv "$summary_tmp" "$summary"
trap - EXIT HUP INT TERM
echo "wrote $summary" >&2
