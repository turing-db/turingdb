import sys
from turingdb import TuringDB, TuringDBException

client = TuringDB(host="localhost")

# S3_CONNECT without a region is a syntax error

try:
    client.query("S3_CONNECT \"ACCESSID\" \"SECRET_KEY\" ")
except TuringDBException as e:
    message = str(e)
    if not message.startswith("PARSE_ERROR: ") or "syntax error" not in message:
        print(f"Unexpected error message: {message!r}")
        sys.exit(1)
except Exception as e:
    print(f"Invalid exception caught {e}")
    sys.exit(1)
else:
    print("S3_CONNECT without a region raised no error")
    sys.exit(1)

client.query("LIST GRAPH")
