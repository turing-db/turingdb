from turingdb import TuringDB

import create_after_delete_test        as CaD_test
import delete_after_create_test        as DaC_test
import delete_after_delete_test        as DaD_test
import delete_node_after_update_edge_test as DNaUE_test
import set_after_set_test              as SAS_test


def main():
    client = TuringDB(host="http://localhost:6666")
    client.try_reach()
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

    print("Running DELETE-NODE-after-UPDATE-EDGE test")
    DNaUE_test.run(client)
    print("Passed DELETE-NODE-after-UPDATE-EDGE test")

    print("Running SET-after-SET test")
    SAS_test.run(client)
    print("Passed SET-after-SET test")

if __name__ == "__main__":
    main()
