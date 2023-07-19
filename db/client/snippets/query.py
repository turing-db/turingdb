import sys
from turingdb import Turing, ValueType

if __name__ == "__main__":
    turing = Turing("localhost:6666")

    # Checking if server is up and running
    if not turing.running:
        print("Server is not running. Exiting")
        sys.exit()

    turing.execute_query('list databases')
