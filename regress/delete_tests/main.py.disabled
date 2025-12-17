from turingdb import TuringDB
import rebased_tombstones_test as rebase_test

def main() -> None:
  client = TuringDB(host="http://localhost:6666")
  print("Connected to DB")

  print("Running Tombstone rebase tests")
  rebase_test.run(client)
  print("Passed Tombstone rebase tests")
  

if __name__ == "__main__":
  main()
