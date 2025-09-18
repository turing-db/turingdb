import sys
from turingdb import TuringDB, TuringDBException

client = TuringDB(host="http://localhost:6666")

#Missing Region Name Causes Error

try:
    client.query("S3_CONNECT \"ACCESSID\" \"SECRET_KEY\" ")
except TuringDBException as e:
    sys.exit(0)
except Exception as e:
    sys.exit(1)
