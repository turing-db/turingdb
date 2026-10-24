import json
import os
import shutil
import socket
import subprocess
import time
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TURING_DIR = os.path.join(SCRIPT_DIR, ".turing")
SOCKET_PATH = os.path.join(TURING_DIR, "turingdb.sock")
TIMEOUT_SECONDS = 60
SLOW_GRAPH_NODE_COUNT = 1000000


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def listening(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def start_command(*args):
    return ["turingdb", "start", "-turing-dir", TURING_DIR, "-demon", *args]


def start(*args, env=None):
    result = subprocess.run(start_command(*args), capture_output=True, text=True, timeout=TIMEOUT_SECONDS, env=env)
    return result.returncode, result.stdout + result.stderr


def stop():
    subprocess.run(["turingdb", "stop", "-turing-dir", TURING_DIR], check=True, timeout=TIMEOUT_SECONDS)


def expect_start_failure(args, error):
    returncode, output = start(*args)
    assert returncode == 1, output
    assert error in output, output
    assert "TuringDB failed to start" in output, output
    assert "TuringDB is ready" not in output, output
    print(f"OK: {' '.join(args)} -> {error}")


def rest_query(port, graph, query, change=None):
    url = f"http://127.0.0.1:{port}/query?graph={graph}"
    if change is not None:
        url += f"&change={change}"

    request = urllib.request.Request(url, data=json.dumps({"query": query}).encode(), method="POST")
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        body = json.load(response)

    assert "error" not in body, body
    return body


def create_graph(api_port, graph, node_count):
    rest_env = {name: value for name, value in os.environ.items() if name != "USE_TURING_PROTO"}
    returncode, output = start("-p", str(api_port), env=rest_env)
    assert returncode == 0, output

    rest_query(api_port, "default", f"CREATE GRAPH {graph}")
    change = rest_query(api_port, graph, "CHANGE NEW")["data"][0][0][0]
    rest_query(api_port, graph, f"UNWIND range(1, {node_count}) AS i CREATE (:N {{id: i}})", change)
    rest_query(api_port, graph, "COMMIT", change)
    rest_query(api_port, graph, "CHANGE SUBMIT", change)

    stop()


def expect_stop_during_load_ends_the_daemon(api_port):
    assert not os.path.exists(SOCKET_PATH)
    starting = subprocess.Popen(start_command("-p", str(api_port), "-load", "slow"),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                text=True)

    deadline = time.monotonic() + TIMEOUT_SECONDS
    while not os.path.exists(SOCKET_PATH):
        assert time.monotonic() < deadline, "the daemon never opened its control socket"
        time.sleep(0.005)

    stop()
    output, _ = starting.communicate(timeout=TIMEOUT_SECONDS)

    if starting.returncode == 1:
        assert "TuringDB was stopped before it was ready" in output, output
        print("OK: a stop during -load ends the start")
    else:
        assert starting.returncode == 0, output
        assert "TuringDB is ready" in output, output
        print("OK: a stop that arrived after ready ends the daemon")

    assert not listening(api_port)


def main():
    shutil.rmtree(TURING_DIR, ignore_errors=True)

    with socket.socket() as held:
        held.bind(("127.0.0.1", 0))
        held.listen()
        held_port = held.getsockname()[1]
        api_port = free_port()

        expect_start_failure(["-p", str(api_port), "-ui", "-ui-port", str(held_port)],
                             f"Could not start the visualizer on 127.0.0.1:{held_port}: bind: Address already in use")
        assert not listening(api_port)

        expect_start_failure(["-p", str(held_port)],
                             f"Could not start the server on 127.0.0.1:{held_port}: bind: Address already in use")

        expect_start_failure(["-p", str(api_port), "-load", "nosuchgraph"],
                             "Failed to load graph 'nosuchgraph'")
        assert not listening(api_port)

    ui_port = free_port()
    returncode, output = start("-p", str(api_port), "-ui", "-ui-port", str(ui_port))
    assert returncode == 0, output
    assert "TuringDB is ready" in output, output
    assert listening(api_port)
    assert listening(ui_port)
    print("OK: a start on free ports is ready")
    stop()

    create_graph(api_port, "slow", SLOW_GRAPH_NODE_COUNT)
    expect_stop_during_load_ends_the_daemon(api_port)

    shutil.rmtree(TURING_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
