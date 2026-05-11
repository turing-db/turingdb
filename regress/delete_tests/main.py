from turingdb import TuringClient
import rebased_tombstones_test as rebase_test

def main() -> None:
  client = TuringClient(host="http://localhost:6666")
  client.try_reach()
  print("Connected to DB")

  print("Running Tombstone rebase tests")
  rebase_test.run(client)
  print("Passed Tombstone rebase tests")
  

if __name__ == "__main__":
  main()
