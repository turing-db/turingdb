from turingdb import TuringDB
import create_after_delete_test as CaD_test
import delete_after_create_test as DaC_test
import delete_after_delete_test as DaD_test

def main():
  client = TuringDB(host="http://localhost:6666")
  print("Connected to DB")

  print("Running CREATE-after-DELETE test")
  CaD_test.run(client)
  print("Passed CREATE-after-DELETE test")

  print("Running DELETE-after-CREATE test")
  DaC_test.run(client)
  print("Passed DELETE-after-CREATE test")

  print("Running DELETE-after-DELETE test")
  DaD_test.run(client)
  print("Passed DELETE-after-DELETE test")

if __name__ == "__main__":
  main()
