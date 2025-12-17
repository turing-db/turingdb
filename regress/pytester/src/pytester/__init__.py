from subprocess import Popen
from turingdb import TuringDB
import signal
import time
import os
import shutil


class TuringdbTester:
    DEFAULT_ARGS: list[str] = ["-turing-dir", ".turing", "-nodemon"]

    def __init__(self):
        self._instance: Popen | None = None
        self._client: TuringDB | None = None

    def spawn(self, args: list[str] = DEFAULT_ARGS) -> Popen:
        self.cleanup_turing_dir()
        self.stop()
        args = ["exec", "turingdb"] + args
        print(f"- Spawning turing with cmd: {' '.join(args)}")
        print(f"- Workdir: {os.getcwd()}")
        self._instance = Popen(" ".join(args), shell=True)

        return self._instance

    def stop(self) -> None:
        if self._instance is not None:
            self._instance.send_signal(signal.SIGTERM)
            self._instance.wait()
            return

        self._instance = None
        killproc = Popen("pkill -9 turingdb", shell=True)
        killproc.wait()

    def cleanup_turing_dir(self, path: str = ".turing") -> None:
        if not os.path.exists(path):
            return

        print(f"- Cleaning up old turing dir")
        shutil.rmtree(path)

    def client(self) -> TuringDB:
        if self._client is None:
            self._client = TuringDB(host="http://localhost:6666")
            self.wait_ready(self._client)

        return self._client

    @staticmethod
    def wait_ready(client):
        t0 = time.time()
        while time.time() - t0 < 6:
            try:
                client.try_reach(timeout=1)
                return
            except:
                time.sleep(0.1)
