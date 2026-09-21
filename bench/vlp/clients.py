"""
The graph databases the path benchmark runs against, behind one interface.

A DBClient is opened on one graph, asked one query at a time, and closed. Every
implementation takes the same openCypher text, so a query is written once and asked of
each database as written; nothing here rewrites a query to dodge what an engine cannot
run.

EmbeddedTuringDBClient runs the engine in this process through its python bindings and
times the `TuringDB::query` call itself. TuringDBClient drives the turingdb shell over a
pipe instead, which reports the engine's own execution time but costs a fork per query.
BoltClient speaks bolt, which serves memgraph and neo4j. FalkorClient speaks the redis
protocol.

LadybugClient translates, because ladybug is embedded and its dialect is not openCypher:
`toLadybug` says exactly what it changes. A translation that changed the question shows
up as a disagreeing count in the report, which is what keeps it honest.
"""

import queue
import re
import subprocess
import sys
import threading
import time

from abc import ABC, abstractmethod
from dataclasses import dataclass

SENTINEL = "TURINGBENCHREPLYEND"


@dataclass
class QueryResult:
    wallMilliseconds: float = 0.0
    engineMilliseconds: float = None
    rows: int = None
    value: int = None
    error: str = None

    @property
    def milliseconds(self):
        if self.engineMilliseconds is not None:
            return self.engineMilliseconds

        return self.wallMilliseconds


class DBClient(ABC):
    def __init__(self, name, timeout):
        self.name = name
        self._timeout = timeout

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *_):
        self.close()

    @abstractmethod
    def open(self):
        ...

    @abstractmethod
    def close(self):
        ...

    @abstractmethod
    def run(self, query):
        ...


class TuringDBClient(DBClient):
    """The turingdb shell over a pipe, on one graph of one turing dir."""

    def __init__(self, name, binary, turingDir, graph, port, timeout, loadTimeout=600):
        super().__init__(name, timeout)
        self._binary = binary
        self._turingDir = turingDir
        self._graph = graph
        self._port = port
        self._loadTimeout = loadTimeout
        self._process = None
        self._output = None

    def open(self):
        command = ["stdbuf", "-oL", self._binary, "-turing-dir", self._turingDir, "-p", str(self._port)]
        self._process = subprocess.Popen(command,
                                         stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT,
                                         text=True)

        self._output = queue.Queue()
        threading.Thread(target=pump, args=(self._process.stdout, self._output), daemon=True).start()

        reply = self._ask([f"load graph {self._graph}", f"cd {self._graph}"], self._loadTimeout)

        failures = [line.strip() for line in reply if "[error]" in line or "Could not acquire lock" in line]
        if failures:
            self.close()
            raise RuntimeError(f"{self.name}: loading {self._graph} failed: {failures[0]}")

    def close(self):
        if self._process is None:
            return

        try:
            self._write("exit")
            self._process.wait(timeout=60)
        except Exception:
            self._process.kill()
            self._process.wait()

        self._process = None

    def run(self, query):
        start = time.perf_counter()

        try:
            reply = self._ask([query], self._timeout)
        except TimeoutError:
            self._restart()
            return QueryResult(wallMilliseconds=self._timeout * 1000, error="TIMEOUT")
        except (BrokenPipeError, RuntimeError) as error:
            self._restart()
            return QueryResult(wallMilliseconds=(time.perf_counter() - start) * 1000, error=f"SHELL LOST: {error}"[:120])

        return parseShellReply(reply, (time.perf_counter() - start) * 1000)

    def _write(self, line):
        self._process.stdin.write(line + "\n")
        self._process.stdin.flush()

    def _ask(self, lines, timeout):
        for line in lines:
            self._write(line)

        self._write(f"sh echo {SENTINEL}")

        return self._collect(timeout)

    def _collect(self, timeout):
        deadline = time.monotonic() + timeout
        reply = []

        while True:
            try:
                line = self._output.get(timeout=max(deadline - time.monotonic(), 0.0))
            except queue.Empty:
                raise TimeoutError(f"{self.name}: no reply within {timeout} s")

            if line is None:
                raise RuntimeError(f"{self.name}: the shell exited")

            if line.startswith(SENTINEL):
                return reply

            reply.append(line)

    def _restart(self):
        if self._process is not None:
            self._process.kill()
            self._process.wait()
            self._process = None

        self.open()


class EmbeddedTuringDBClient(DBClient):
    """The engine in this process, through turingdb's python bindings.

    `query_raw` calls straight into `TuringDB::query` and hands the result back as numpy
    columns, so the time measured around it is the query plus one copy per column - no
    shell, no socket, and none of the fork the shell's sentinel pays.
    """

    def __init__(self, name, turingDir, graph, timeout, sdkPath=""):
        super().__init__(name, timeout)
        self._turingDir = turingDir
        self._graph = graph
        self._sdkPath = sdkPath
        self._client = None

    def open(self):
        if self._sdkPath and self._sdkPath not in sys.path:
            sys.path.insert(0, self._sdkPath)

        try:
            from turingdb.embedded_client import EmbeddedClient
        except ImportError as error:
            raise RuntimeError(f"{self.name}: no embedded engine, build python/turingdb/_embedded") from error

        self._client = EmbeddedClient(self._turingDir)
        self._client.query_raw(f"load graph {self._graph}")
        self._client.set_graph(self._graph)

    def close(self):
        self._client = None

    def run(self, query):
        start = time.perf_counter()

        try:
            result = self._client.query_raw(query)
        except Exception as error:
            return QueryResult(wallMilliseconds=(time.perf_counter() - start) * 1000, error=shortenError(error))

        wallMilliseconds = (time.perf_counter() - start) * 1000
        columns = result["data"]
        rows = len(next(iter(columns.values()))) if columns else 0

        return QueryResult(wallMilliseconds=wallMilliseconds, rows=rows, value=columnScalar(columns, rows))


def columnScalar(columns, rows):
    if rows != 1 or len(columns) != 1:
        return None

    only = next(iter(columns.values()))[0]

    return int(only) if isinstance(only, (int, float)) and float(only).is_integer() else None


class BoltClient(DBClient):
    """Any bolt server - memgraph or neo4j - through the neo4j driver."""

    def __init__(self, name, uri, timeout, auth=("", "")):
        super().__init__(name, timeout)
        self._uri = uri
        self._auth = auth
        self._driver = None
        self._session = None

    def open(self):
        try:
            import neo4j
        except ImportError as error:
            raise RuntimeError(f"{self.name}: the bolt driver is missing (pip install neo4j)") from error

        self._driver = neo4j.GraphDatabase.driver(self._uri, auth=self._auth)
        self._driver.verify_connectivity()
        self._session = self._driver.session()

    def close(self):
        if self._session is not None:
            self._session.close()
            self._session = None

        if self._driver is not None:
            self._driver.close()
            self._driver = None

    def run(self, query):
        start = time.perf_counter()

        try:
            with self._session.begin_transaction(timeout=self._timeout) as transaction:
                result = transaction.run(query)
                records = list(result)
                summary = result.consume()
        except Exception as error:
            self._reopenSession()
            return QueryResult(wallMilliseconds=(time.perf_counter() - start) * 1000, error=shortenError(error))

        wallMilliseconds = (time.perf_counter() - start) * 1000

        return QueryResult(wallMilliseconds=wallMilliseconds,
                           engineMilliseconds=serverMilliseconds(summary),
                           rows=len(records),
                           value=scalarValue(records))

    def _reopenSession(self):
        self._session.close()
        self._session = self._driver.session()


class LadybugClient(DBClient):
    """ladybug, embedded in this process, on the database directory `database`.

    `nodeTable` is the table every node lives in when one table holds several labels, as
    the reactome fixture does; leave it empty where a label is a table of its own.
    """

    def __init__(self, name, database, nodeTable, timeout, maxDepth=200, bufferPool=8 * 1024**3):
        super().__init__(name, timeout)
        self._databasePath = database
        self._nodeTable = nodeTable
        self._maxDepth = maxDepth
        self._bufferPool = bufferPool
        self._database = None
        self._connection = None

    def open(self):
        try:
            import ladybug
        except ImportError as error:
            raise RuntimeError(f"{self.name}: ladybug is not installed (pip install ladybug)") from error

        self._database = ladybug.Database(self._databasePath, buffer_pool_size=self._bufferPool)
        self._connection = ladybug.Connection(self._database)
        self._connection.execute(f"CALL var_length_extend_max_depth={self._maxDepth}")
        self._connection.set_query_timeout(int(self._timeout * 1000))

    def close(self):
        self._connection = None
        self._database = None

    def run(self, query):
        start = time.perf_counter()

        try:
            result = self._connection.execute(toLadybug(query, self._nodeTable, self._maxDepth))
            rows, value = drainLadybugResult(result)
        except Exception as error:
            return QueryResult(wallMilliseconds=(time.perf_counter() - start) * 1000, error=shortenError(error))

        return QueryResult(wallMilliseconds=(time.perf_counter() - start) * 1000, rows=rows, value=value)


class FalkorClient(DBClient):
    """FalkorDB over the redis protocol, one graph key per fixture."""

    def __init__(self, name, host, port, graph, timeout):
        super().__init__(name, timeout)
        self._host = host
        self._port = port
        self._graphName = graph
        self._graph = None

    def open(self):
        try:
            from falkordb import FalkorDB
        except ImportError as error:
            raise RuntimeError(f"{self.name}: the falkordb client is missing (pip install falkordb)") from error

        database = FalkorDB(host=self._host, port=self._port)
        if self._graphName not in database.list_graphs():
            raise RuntimeError(f"{self.name}: no graph {self._graphName} on {self._host}:{self._port}")

        self._graph = database.select_graph(self._graphName)

    def close(self):
        self._graph = None

    def run(self, query):
        start = time.perf_counter()

        try:
            result = self._graph.query(query, timeout=int(self._timeout * 1000))
        except Exception as error:
            return QueryResult(wallMilliseconds=(time.perf_counter() - start) * 1000, error=shortenError(error))

        return QueryResult(wallMilliseconds=(time.perf_counter() - start) * 1000,
                           engineMilliseconds=result.run_time_ms,
                           rows=len(result.result_set),
                           value=scalarValue(result.result_set))


# openCypher as this benchmark writes it, in ladybug's dialect: a quantifier becomes
# `* TRAIL 1..k`, unbounded becomes the recursion ceiling, and a label becomes a boolean
# predicate where `nodeTable` holds more than one of them.
def toLadybug(query, nodeTable, maxDepth):
    head, _, projection = query.rpartition(" RETURN ")
    pattern, _, where = head.partition(" WHERE ")

    predicates = []

    def rewriteNode(node):
        variable, label, properties = node.group(1), node.group(2), node.group(3)

        if nodeTable:
            predicates.append(f"{variable}.is{label}")

        if properties:
            for name, value in re.findall(r"(\w+)\s*:\s*([^,}]+)", properties):
                predicates.append(f"{variable}.{name} = {value.strip()}")

        return f"({variable}:{nodeTable or label})"

    pattern = re.sub(r"\((\w+):(\w+)(\s*\{[^}]*\})?\)", rewriteNode, pattern)
    pattern = re.sub(r"\*(\d+)\.\.(\d+)(?=\])", r"* TRAIL \1..\2", pattern)
    pattern = re.sub(r"\*(?=\])", f"* TRAIL 1..{maxDepth}", pattern)

    if where:
        predicates.append(where)

    if not predicates:
        return f"{pattern} RETURN {projection}"

    return f"{pattern} WHERE {' AND '.join(predicates)} RETURN {projection}"


def drainLadybugResult(result):
    rows = result.get_num_tuples()
    value = None

    if rows == 1 and len(result.get_column_names()) == 1:
        first = result.get_next()
        value = first[0] if isinstance(first[0], int) else None
    else:
        while result.has_next():
            result.get_next()

    result.close()

    return rows, value


def pump(stream, output):
    for line in iter(stream.readline, ""):
        output.put(line)

    output.put(None)


# The status code and the deepest line of a turingdb shell error, "PARSE_ERROR: Not implemented: IN"
def parseShellError(reply):
    code = None
    detail = None

    for line in reply:
        if "[error]" in line and code is None:
            code = line.split("[error]", 1)[1].split(":", 1)[0].strip()

        deeper = re.match(r"^-------\*\s*(.+)", line)
        if deeper:
            detail = deeper.group(1).strip()

    if code is None:
        return None

    if detail is None:
        return code

    return f"{code}: {detail}"[:120]


def parseShellReply(reply, wallMilliseconds):
    result = QueryResult(wallMilliseconds=wallMilliseconds, error=parseShellError(reply))

    for line in reply:
        rows = re.match(r"Query returned (\d+) rows\.", line)
        if rows:
            result.rows = int(rows.group(1))
            continue

        elapsed = re.match(r"Query executed in ([0-9.]+) ms\.", line)
        if elapsed:
            result.engineMilliseconds = float(elapsed.group(1))
            continue

        value = re.match(r"^\|\s*(-?\d+)\s*\|$", line.strip())
        if value:
            result.value = int(value.group(1))

    return result


def serverMilliseconds(summary):
    available = summary.result_available_after
    consumed = summary.result_consumed_after

    if available is None or consumed is None:
        return None

    return float(available + consumed)


def scalarValue(records):
    if len(records) != 1 or len(records[0]) != 1:
        return None

    only = records[0][0]

    return only if isinstance(only, int) else None


def shortenError(error):
    text = str(error).replace("\n", " ")

    if "timeout" in text.lower() or "timed out" in text.lower() or "interrupt" in text.lower():
        return "TIMEOUT"

    return text[:120]
