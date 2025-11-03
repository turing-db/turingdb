from turingdb import TuringDB
import cad

def main():
  client = TuringDB(host="http://localhost:6666")
  print("Connected to DB")

  print("Running CREATE-after-DELETE test")
  cad.run(client)

if __name__ == "__main__":
  main()
