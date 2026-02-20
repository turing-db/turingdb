from turingdb import TuringDB

import pandas as pd

import subprocess
import signal
import os
import shutil
import time

GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
NC = "\033[0m"


def spawn_turingdb_process():
    cmd = "turingdb -demon -turing-dir .turing"
    print(f"- {GREEN}Starting turingdb with `{BLUE}{cmd}{NC}`{NC}")
    return subprocess.Popen(cmd, shell=True)


def wait_port(port):
    import socket

    for _ in range(100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return True
        time.sleep(0.1)

    return False


def stop_with_signal(proc):
    cmd = "pkill -2 turingdb"
    print(f"- {GREEN}Stopping turingdb with `{BLUE}{cmd}{NC}`{NC}")
    subprocess.call(cmd, shell=True)
    assert wait_port(6666)


def stop_with_cmd(proc):
    cmd = "turingdb stop -turing-dir .turing"
    print(f"- {GREEN}Stopping turingdb with `{BLUE}{cmd}{NC}`{NC}")
    subprocess.call(cmd, shell=True)
    assert wait_port(6666)


def wait_ready(client):
    t0 = time.time()
    while time.time() - t0 < 6:
        try:
            client.try_reach(timeout=1)
            return
        except:
            time.sleep(1)

    raise RuntimeError("Failed to connect to turingdb")


if __name__ == "__main__":
    if os.path.exists(".turing"):
        shutil.rmtree(".turing")

    #  Start and stop with signal
    proc = spawn_turingdb_process()
    client = TuringDB(host="http://localhost:6666")
    wait_ready(client)
    stop_with_signal(proc)

    proc = spawn_turingdb_process()
    wait_ready(client)
    stop_with_cmd(proc)
