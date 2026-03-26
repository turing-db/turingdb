#!/usr/bin/env python3
"""
Triage script for cypher fuzz pipeline crashes.

For each crash input found by AFL++:
  1. Runs the fuzz_query_engine harness under valgrind to reproduce the crash
  2. If valgrind doesn't reproduce it, falls back to GDB batch mode
  3. Extracts the stacktrace from whichever tool caught it
  4. Deduplicates by (source_file, crash_type, line_number)
  5. Stores unique crashes in an SQLite database

Usage:
    python3 fuzz/triage_crashes.py [options]

Options:
    --crashes-dir DIR    Directory containing AFL crash inputs
                         (default: fuzz_results/afl_findings/fuzz_query_engine/default/crashes)
    --harness PATH       Path to the fuzz_query_engine binary
                         (default: build_debug/fuzz/fuzz_query_engine)
    --db PATH            Path to the SQLite database
                         (default: fuzz_results/crash_triage.db)
    --timeout SECS       Per-crash timeout in seconds (default: 30)
    --verbose            Print detailed output for each crash
"""

import argparse
import os
import re
import sqlite3
import subprocess
import sys


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULT_CRASHES_DIR = os.path.join(
    REPO_ROOT, "fuzz_results", "afl_findings", "fuzz_query_engine", "default", "crashes"
)
DEFAULT_HARNESS = os.path.join(REPO_ROOT, "build_debug", "fuzz", "fuzz_query_engine")
DEFAULT_DB = os.path.join(REPO_ROOT, "fuzz_results", "crash_triage.db")
DEFAULT_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crashes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file TEXT NOT NULL,
            crash_type TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            first_input TEXT NOT NULL,
            full_stacktrace TEXT NOT NULL,
            input_content TEXT,
            tool_output TEXT,
            signal TEXT,
            tool TEXT,
            timestamp TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(source_file, crash_type, line_number)
        )
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Running the harness
# ---------------------------------------------------------------------------

def read_crash_input(crash_file):
    with open(crash_file, "rb") as f:
        return f.read()


def run_valgrind(harness, input_data, timeout):
    """Run harness under valgrind. Returns (stderr_text, returncode) or (None, None) on timeout."""
    try:
        result = subprocess.run(
            [
                "valgrind",
                "--error-exitcode=1",
                "--track-origins=yes",
                "--num-callers=30",
                harness,
            ],
            input=input_data,
            capture_output=True,
            timeout=timeout,
        )
        return result.stderr.decode("utf-8", errors="replace"), result.returncode
    except subprocess.TimeoutExpired:
        return None, None


def run_gdb(harness, input_data, timeout):
    """Run harness under GDB batch mode. Returns (gdb_output, returncode) or (None, None) on timeout."""
    try:
        result = subprocess.run(
            [
                "gdb", "-batch", "-q",
                "-ex", "set confirm off",
                "-ex", "set pagination off",
                "-ex", "run",
                "-ex", "bt full 30",
                "-ex", "info registers",
                "-ex", "quit",
                "--args", harness,
            ],
            input=input_data,
            capture_output=True,
            timeout=timeout,
        )
        output = result.stdout.decode("utf-8", errors="replace")
        output += result.stderr.decode("utf-8", errors="replace")
        return output, result.returncode
    except subprocess.TimeoutExpired:
        return None, None


# ---------------------------------------------------------------------------
# Valgrind output parsing
# ---------------------------------------------------------------------------

def valgrind_crashed(output, returncode):
    """Check if valgrind actually caught a crash (not a clean exit)."""
    if output is None:
        return False
    if "Process terminating with default action of signal" in output:
        return True
    if re.search(r"Invalid (read|write) of size", output):
        return True
    # Non-zero exit from the program itself (SIGABRT = 134, SIGSEGV = 139)
    if returncode not in (0, 1):
        return True
    return False


def parse_valgrind_output(output):
    """Parse valgrind stderr into structured crash info."""
    if not output:
        return None

    signal = None
    m = re.search(r"Process terminating with default action of signal \d+ \((\w+)\)", output)
    if m:
        signal = m.group(1)

    crash_type = _classify_crash_type(output, signal)

    # Valgrind frames: ==PID==    at/by 0xADDR: function (file.cpp:123)
    frame_re = re.compile(r"==\d+==\s+(?:at|by) 0x[0-9A-Fa-f]+: (.+?) \(([^)]+)\)")
    frames = []
    for match in frame_re.finditer(output):
        func = match.group(1)
        loc = match.group(2)
        if loc.startswith("in "):
            frames.append({"function": func, "source_file": loc[3:], "line_number": 0})
        else:
            parts = loc.rsplit(":", 1)
            try:
                lineno = int(parts[1]) if len(parts) > 1 else 0
            except ValueError:
                lineno = 0
            frames.append({"function": func, "source_file": parts[0], "line_number": lineno})

    return {"frames": frames, "crash_type": crash_type, "signal": signal}


# ---------------------------------------------------------------------------
# GDB output parsing
# ---------------------------------------------------------------------------

def gdb_crashed(output, returncode):
    """Check if GDB caught a signal."""
    if output is None:
        return False
    if re.search(r"Program received signal (SIG\w+)", output):
        return True
    return False


def parse_gdb_output(output):
    """Parse GDB batch-mode backtrace into structured crash info."""
    if not output:
        return None

    signal = None
    m = re.search(r"Program received signal (SIG\w+)", output)
    if m:
        signal = m.group(1)

    crash_type = _classify_crash_type(output, signal)

    # GDB frames: #N  0xADDR in function (args) at file.cpp:123
    # or:         #N  0xADDR in function (args) from /lib/foo.so
    # or:         #N  function (args) at file.cpp:123
    frame_re = re.compile(
        r"#(\d+)\s+"
        r"(?:0x[0-9a-fA-F]+\s+in\s+)?"
        r"(.+?)\s+"
        r"(?:at\s+([^:\s]+):(\d+)|from\s+(\S+))"
    )
    frames = []
    for match in frame_re.finditer(output):
        func = match.group(2).split("(")[0].strip()
        if match.group(3):
            frames.append({
                "function": func,
                "source_file": match.group(3),
                "line_number": int(match.group(4)),
            })
        elif match.group(5):
            frames.append({
                "function": func,
                "source_file": match.group(5),
                "line_number": 0,
            })

    return {"frames": frames, "crash_type": crash_type, "signal": signal}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _classify_crash_type(output, signal):
    """Determine crash type from tool output and signal."""
    error_patterns = [
        (r"Invalid (read|write) of size \d+", "invalid_memory_access"),
        (r"Use of uninitialised value", "uninitialized_value"),
        (r"Conditional jump or move depends on uninitialised value", "uninitialized_value"),
        (r"Invalid free\(\)", "invalid_free"),
        (r"Mismatched free\(\) / delete / delete \[\]", "mismatched_free"),
        (r"heap-buffer-overflow", "heap_buffer_overflow"),
        (r"stack-buffer-overflow", "stack_buffer_overflow"),
        (r"heap-use-after-free", "use_after_free"),
        (r"null-pointer-dereference", "null_deref"),
    ]
    for pattern, ctype in error_patterns:
        if re.search(pattern, output):
            return ctype

    signal_map = {
        "SIGSEGV": "segfault",
        "SIGABRT": "abort",
        "SIGFPE": "floating_point_exception",
        "SIGBUS": "bus_error",
    }
    if signal and signal in signal_map:
        return signal_map[signal]
    if signal:
        return signal.lower()
    return "unknown"


def find_crash_origin(parsed):
    """Find the top application-level frame that identifies this crash.

    Skips system frames, C++ runtime, and assertion/exception infrastructure
    (BioAssert, Panic, FatalException, TuringException) to reach the actual
    caller that triggered the crash.
    """
    if not parsed or not parsed["frames"]:
        return None

    skip_prefixes = ("/usr/", "/lib/", "/build/", "vg_replace")
    skip_functions = (
        "__GI_raise", "__GI_abort", "raise", "abort",
        "__libc_", "_Exit", "__cxa_throw", "__cxa_rethrow",
        "__pthread_kill", "pthread_kill", "gsignal",
    )
    # Assertion/exception infrastructure files — the crash origin is the caller,
    # not the throw/assert site itself.
    skip_source_basenames = (
        "BioAssert.cpp", "BioAssert.h",
        "Panic.h",
        "FatalException.h", "FatalException.cpp",
        "TuringException.h", "TuringException.cpp",
    )
    # STL / libstdc++ implementation headers — the bug is in the caller, not
    # in std::vector::emplace_back or similar.
    skip_source_patterns = (
        "vector.tcc", "stl_vector.h", "stl_tree.h", "stl_map.h",
        "stl_set.h", "stl_list.h", "stl_deque.h", "stl_uninitialized.h",
        "stl_construct.h", "stl_algo.h", "stl_algobase.h",
        "shared_ptr.h", "unique_ptr.h", "allocator.h",
        "basic_string.h", "basic_string.tcc", "char_traits.h",
        "new_allocator.h", "alloc_traits.h",
        "hashtable.h", "hashtable_policy.h",
    )

    for frame in parsed["frames"]:
        src = frame["source_file"]
        func = frame["function"]
        if any(src.startswith(p) for p in skip_prefixes):
            continue
        if any(func.startswith(p) for p in skip_functions):
            continue
        src_basename = os.path.basename(src)
        if src_basename in skip_source_basenames:
            continue
        if src_basename in skip_source_patterns:
            continue
        if frame["line_number"] == 0:
            continue
        return (src, parsed["crash_type"], frame["line_number"])

    # Fallback: first frame with a line number
    for frame in parsed["frames"]:
        if frame["line_number"] > 0:
            return (frame["source_file"], parsed["crash_type"], frame["line_number"])

    # Last resort: first frame at all
    if parsed["frames"]:
        f = parsed["frames"][0]
        return (f["source_file"], parsed["crash_type"], f["line_number"])

    return None


def format_stacktrace(parsed):
    if not parsed or not parsed["frames"]:
        return ""
    lines = []
    for i, frame in enumerate(parsed["frames"]):
        loc = frame["source_file"]
        if frame["line_number"] > 0:
            loc += f":{frame['line_number']}"
        lines.append(f"#{i:<3d} {frame['function']} ({loc})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# File listing
# ---------------------------------------------------------------------------

def get_crash_files(crashes_dir):
    files = []
    for entry in os.listdir(crashes_dir):
        path = os.path.join(crashes_dir, entry)
        if os.path.isfile(path) and not entry.startswith(".") and entry != "README.txt":
            files.append(path)

    def sort_key(path):
        m = re.search(r"id:(\d+)", os.path.basename(path))
        return int(m.group(1)) if m else 0

    files.sort(key=sort_key)
    return files


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Triage AFL++ cypher fuzzer crashes via valgrind / GDB"
    )
    parser.add_argument("--crashes-dir", default=DEFAULT_CRASHES_DIR,
                        help="Directory containing AFL crash inputs")
    parser.add_argument("--harness", default=DEFAULT_HARNESS,
                        help="Path to fuzz_query_engine binary")
    parser.add_argument("--db", default=DEFAULT_DB,
                        help="Path to SQLite triage database")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help="Per-crash timeout in seconds")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print detailed output for each crash")
    args = parser.parse_args()

    if not os.path.isdir(args.crashes_dir):
        print(f"Error: crashes directory not found: {args.crashes_dir}", file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(args.harness):
        print(f"Error: harness binary not found: {args.harness}", file=sys.stderr)
        print("Build with: cd build_debug && DEBUG_BUILD=1 cmake .. && make -j8 fuzz_query_engine",
              file=sys.stderr)
        sys.exit(1)

    # Check tools
    have_valgrind = True
    try:
        subprocess.run(["valgrind", "--version"], capture_output=True, check=True)
    except FileNotFoundError:
        have_valgrind = False
        print("Warning: valgrind not found, will use GDB only", file=sys.stderr)

    have_gdb = True
    try:
        subprocess.run(["gdb", "--version"], capture_output=True, check=True)
    except FileNotFoundError:
        have_gdb = False
        if not have_valgrind:
            print("Error: neither valgrind nor gdb found", file=sys.stderr)
            sys.exit(1)

    crash_files = get_crash_files(args.crashes_dir)
    if not crash_files:
        print("No crash files found.")
        return

    print(f"Found {len(crash_files)} crash inputs in {args.crashes_dir}")
    print(f"Database: {args.db}")
    print(f"Harness:  {args.harness}")
    print(f"Tools:    {'valgrind' if have_valgrind else ''}"
          f"{' + ' if have_valgrind and have_gdb else ''}"
          f"{'gdb' if have_gdb else ''}")
    print()

    conn = init_db(args.db)
    new_count = 0
    dup_count = 0
    no_repro_count = 0

    for i, crash_file in enumerate(crash_files, 1):
        filename = os.path.basename(crash_file)
        print(f"[{i}/{len(crash_files)}] {filename} ... ", end="", flush=True)

        input_data = read_crash_input(crash_file)
        parsed = None
        tool_output = None
        tool_name = None

        # Phase 1: try valgrind
        if have_valgrind:
            vg_output, vg_rc = run_valgrind(args.harness, input_data, args.timeout)
            if vg_output is None:
                print("TIMEOUT (valgrind)")
                no_repro_count += 1
                continue
            if valgrind_crashed(vg_output, vg_rc):
                parsed = parse_valgrind_output(vg_output)
                tool_output = vg_output
                tool_name = "valgrind"

        # Phase 2: fall back to GDB if valgrind didn't catch it
        if parsed is None and have_gdb:
            gdb_output, gdb_rc = run_gdb(args.harness, input_data, args.timeout)
            if gdb_output is None:
                print("TIMEOUT (gdb)")
                no_repro_count += 1
                continue
            if gdb_crashed(gdb_output, gdb_rc):
                parsed = parse_gdb_output(gdb_output)
                tool_output = gdb_output
                tool_name = "gdb"

        if parsed is None:
            print("NO REPRO")
            no_repro_count += 1
            if args.verbose and tool_output:
                for line in tool_output.strip().split("\n")[-10:]:
                    print(f"    {line}")
            continue

        origin = find_crash_origin(parsed)
        if origin is None:
            print(f"NO STACKTRACE ({tool_name})")
            no_repro_count += 1
            if args.verbose and tool_output:
                for line in tool_output.strip().split("\n")[-15:]:
                    print(f"    {line}")
            continue

        source_file, crash_type, line_number = origin
        stacktrace = format_stacktrace(parsed)

        # Dedup check
        existing = conn.execute(
            "SELECT id FROM crashes WHERE source_file = ? AND crash_type = ? AND line_number = ?",
            (source_file, crash_type, line_number),
        ).fetchone()

        if existing:
            print(f"DUP (#{existing[0]}: {crash_type} at {source_file}:{line_number})")
            dup_count += 1
        else:
            input_content = input_data.decode("utf-8", errors="replace")
            conn.execute(
                """INSERT INTO crashes
                   (source_file, crash_type, line_number, first_input, full_stacktrace,
                    input_content, tool_output, signal, tool)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    source_file, crash_type, line_number, filename, stacktrace,
                    input_content, tool_output, parsed.get("signal"), tool_name,
                ),
            )
            conn.commit()
            crash_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            new_count += 1
            print(f"NEW #{crash_id} — {crash_type} at {source_file}:{line_number} [{tool_name}]")

            if args.verbose:
                print(f"  Signal: {parsed.get('signal')}")
                print(f"  Input:  {input_content[:120]}...")
                print(f"  Stack:")
                for line in stacktrace.split("\n")[:10]:
                    print(f"    {line}")
                print()

    print()
    print(f"Done. {new_count} new, {dup_count} duplicates, {no_repro_count} not reproducible.")
    print(f"Database: {args.db}")

    # Generate markdown report
    report_path = os.path.join(os.path.dirname(args.db), "crash_triage_report.md")
    generate_report(conn, report_path, no_repro_count, dup_count)
    conn.close()

    print(f"Report:   {report_path}")


def generate_report(conn, report_path, no_repro_count, dup_count):
    """Write a markdown summary of all unique crashes in the database."""
    rows = conn.execute(
        """SELECT id, source_file, crash_type, line_number, first_input,
                  full_stacktrace, signal, tool
           FROM crashes ORDER BY id"""
    ).fetchall()

    total_inputs = no_repro_count + dup_count + len(rows)

    with open(report_path, "w") as f:
        f.write("# Cypher Fuzz Crash Triage Report\n\n")
        f.write(f"| Metric | Count |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| Total crash inputs | {total_inputs} |\n")
        f.write(f"| Unique crashes | {len(rows)} |\n")
        f.write(f"| Duplicates | {dup_count} |\n")
        f.write(f"| Not reproducible | {no_repro_count} |\n")
        f.write("\n")

        if not rows:
            f.write("No reproducible crashes found.\n")
            return

        f.write("## Unique Crashes\n\n")
        f.write("| # | Type | Signal | Source | Line | Tool | First Input |\n")
        f.write("|---|------|--------|--------|------|------|-------------|\n")
        for row in rows:
            crash_id, src, ctype, lineno, first_input, _, sig, tool = row
            sig = sig or "-"
            src_short = os.path.basename(src)
            input_short = first_input[:50]
            f.write(f"| {crash_id} | {ctype} | {sig} | {src_short} | {lineno} | {tool} | `{input_short}` |\n")

        f.write("\n## Stacktraces\n")
        for row in rows:
            crash_id, src, ctype, lineno, first_input, stacktrace, sig, tool = row
            sig = sig or "unknown"
            f.write(f"\n### Crash #{crash_id} — {ctype} ({sig}) at {src}:{lineno}\n\n")
            f.write(f"**Input:** `{first_input}`\\\n")
            f.write(f"**Tool:** {tool}\n\n")
            f.write("```\n")
            f.write(stacktrace)
            f.write("\n```\n")


if __name__ == "__main__":
    main()
