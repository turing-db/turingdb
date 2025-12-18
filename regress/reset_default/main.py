import os
import time
import shutil
from pytester import TuringdbTester
from turingdb import TuringDBException

tester = TuringdbTester()

INVALID_DIR = "turing-invalid"
MISSING_DIR = "turing-missing"


def should_be_running(tester):
    time.sleep(0.3)
    if tester.returncode is not None:
        print(f"!! Failure, should be running")
        exit(1)
    tester.stop()


def should_have_stopped(tester):
    time.sleep(0.3)
    if tester.returncode is None:
        print(f"!! Failure, should have stopped")
        tester.stop()
        exit(1)


# Reset default
if os.path.exists(f"{INVALID_DIR}"):
    print(f"- Removing invalid default")
    shutil.rmtree(f"{INVALID_DIR}")

print(f"- Writing invalid default")
print(os.getcwd())

try:
    os.makedirs(f"{INVALID_DIR}/graphs")
except:
    pass

with open(f"{INVALID_DIR}/graphs/default", "w") as f:
    f.write("UNLOADABLE")

# Test invalid default
print(f"- Test invalid default")
tester.spawn(args=["-turing-dir", INVALID_DIR, "-nodemon"], cleanup=False)
should_have_stopped(tester)

print(f"- Test resetting default while invalid")
tester.spawn(args=["-turing-dir", INVALID_DIR, "-nodemon", "-reset-default"])
should_be_running(tester)


# Test missing default
if os.path.exists(f"{MISSING_DIR}/graphs/default"):
    shutil.rmtree(f"{MISSING_DIR}/graphs/default")

print(f"- Test missing default")
tester.spawn(args=["-turing-dir", MISSING_DIR, "-nodemon"])
should_be_running(tester)

# Test resetting default
print(f"- Test resetting default while missing")
tester.spawn(args=["-turing-dir", MISSING_DIR, "-nodemon", "-reset-default"])
should_be_running(tester)
