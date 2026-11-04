import socket
import sys
import time

from turingdb import TuringDB, TuringDBException


# A server that accepts the connection and never answers.
listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.bind(("127.0.0.1", 0))
listener.listen(4)
port = listener.getsockname()[1]

client = TuringDB(host="127.0.0.1", port=port)

for method in (client.try_reach, client.warmup):
    start = time.monotonic()
    try:
        method(timeout=0.5)
    except TuringDBException as e:
        elapsed = time.monotonic() - start
        if "timed out after 500 ms" not in str(e):
            print(f"{method.__name__}: unexpected error {e!r}")
            sys.exit(1)
        if elapsed > 5:
            print(f"{method.__name__}: took {elapsed:.1f} s for a 0.5 s timeout")
            sys.exit(1)
    else:
        print(f"{method.__name__}: a silent server raised no error")
        sys.exit(1)

    print(f"{method.__name__}: timed out after {elapsed:.2f} s")

print("* try_reach_timeout: done")
