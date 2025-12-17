import os
import shutil
from pytester import TuringdbTester
from turingdb import TuringDBException

tester = TuringdbTester()

INVALID_DIR = ".turing-invalid"
MISSING_DIR = ".turing-missing"

# Reset default
if os.path.exists(f"{INVALID_DIR}/graphs/default"):
    print(f"- Removing invalid default")
    shutil.rmtree(f"{INVALID_DIR}/graphs/default")

    print(f"- Writing invalid default")
    with open(f"{INVALID_DIR}/graphs/default", "w") as f:
        f.write("UNLOADABLE")

# Test invalid default
print(f"- Test invalid default")
tester.spawn(args=["-turing-dir", INVALID_DIR, "-nodemon"])
tester.stop()
print(tester._instance.returncode)
if tester._instance.returncode == 0:
    print(f"Failure, should have thrown exception")
    exit(1)

print(f"- Test resetting default while invalid")
# Test resetting default
tester.spawn(args=["-turing-dir", INVALID_DIR, "-nodemon", "-reset-default"])
tester.stop()
if tester._instance.returncode != 0:
    print(f"Failure, should have exited with 0")
    exit(1)

# Test missing default
if os.path.exists(f"{MISSING_DIR}/graphs/default"):
    shutil.rmtree(f"{MISSING_DIR}/graphs/default")

print(f"- Test missing default")
tester.spawn(args=["-turing-dir", MISSING_DIR, "-nodemon"])
tester.stop()
if tester._instance.returncode == 0:
    print(f"Failure, should have thrown exception")
    exit(1)

print(f"- Test resetting default while missing")
# Test resetting default
tester.spawn(args=["-turing-dir", MISSING_DIR, "-nodemon", "-reset-default"])
tester.stop()
if tester._instance.returncode != 0:
    print(f"Failure, should have exited with 0")
    exit(1)
