from turingdb import TuringDB

def main() -> None:
  client = TuringDB(host="localhost")
  try:
    client.query("list graph") # Check the DB is running and listening
  except Exception:
    raise Exception("TuringDB is not running") from None

if __name__ == "__main__":
  main()
