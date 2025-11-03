from turingdb import TuringDB
import create_after_delete_test as CaD_test

def main():
  client = TuringDB(host="http://localhost:6666")
  print("Connected to DB")

  print("Running CREATE-after-DELETE test")
  CaD_test.run(client)
  print("Passed CREATE-after-DELETE test")

if __name__ == "__main__":
  main()
