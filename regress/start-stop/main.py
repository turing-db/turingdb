from turingdb import TuringClient

import subprocess
import os
import shutil
import time

GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
NC = "\033[0m"


def listening_port(port):
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", port)) == 0:
            return True

    return False


def spawn_turingdb_process():
    cmd = "turingdb -demon -turing-dir .turing"
    print(f"- {GREEN}Starting turingdb with `{BLUE}{cmd}{NC}`{NC}")
    assert subprocess.call(cmd, shell=True) == 0
    assert listening_port(6666)


def stop_with_signal(proc):
    cmd = "pkill -2 turingdb"
    print(f"- {GREEN}Stopping turingdb with `{BLUE}{cmd}{NC}`{NC}")
    assert subprocess.call(cmd, shell=True) == 0
    assert wait_port_avail(6666)


def wait_port_avail(port):
    count = 0
    while count < 10:
        if not listening_port(port):
            return True

        time.sleep(0.1)
        count += 1

    return False


def stop_with_cmd(proc):
    cmd = "turingdb stop -turing-dir .turing"
    print(f"- {GREEN}Stopping turingdb with `{BLUE}{cmd}{NC}`{NC}")
    assert subprocess.call(cmd, shell=True) == 0
    assert not listening_port(6666)


if __name__ == "__main__":
    if os.path.exists(".turing"):
        shutil.rmtree(".turing")

    #  Start and stop with signal
    proc = spawn_turingdb_process()
    client = TuringClient()
    stop_with_signal(proc)

    # Start and stop with cmd
    proc = spawn_turingdb_process()
    stop_with_cmd(proc)
