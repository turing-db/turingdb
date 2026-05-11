from turingdb import TuringClient

def main() -> None:
  client = TuringClient(host="http://localhost:6666")
  try:
    client.query("list graph") # Check the DB is running and listening
  except Exception:
    raise Exception("TuringClient is not running") from None

if __name__ == "__main__":
  main()
