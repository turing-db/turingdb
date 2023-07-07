import sys
import turingapi
import grpc
from turingapi.core import ValueType

if __name__ == "__main__":
    turing = turingapi.Turing("localhost:6666")

    # Checking if server is up and running
    if not turing.running:
        print("Server is not running. Exiting")
        sys.exit()

    # List available DBs Request
    res = turing.list_available_databases()

    print("Available databases:")
    for db_name in res.data:
        print(f"  - {db_name}")

    loaded_dbs = turing.list_loaded_databases().data
    print("Loaded databases:")
    for db_name, db in loaded_dbs.items():
        print(f"  - {db_name}: {db}")

    # The recommendations db is available from the previous list_loaded_databases
    # call, but it can also be retrieved from this call:
    db = turing.get_db(db_name="recommendations.db").data
    print(f"Recommendations.db:  - DB Object: {db}")

    if "custom" in loaded_dbs.keys():
        loaded_dbs["custom"].unload()

    # Creating a database:
    print("Creating db 'custom':")
    res = turing.create_db(db_name="custom")
    if res.failed() and res.status_code == grpc.StatusCode.ALREADY_EXISTS:
        res = turing.get_db(db_name="custom")
    db = res.data

    # Uncomment to dump DB:
    # res = db.dump()

    # Creating NodeTypes
    print("Creating node types 'NT' and 'NT2'")
    res = db.create_node_type("NT")
    res = db.create_node_type("NT2")

    # Listing NodeTypes
    print("Listing node types")
    res = db.list_node_types()
    for name, nt in res.data.items():
        print(f"  - {name} [{nt.id}]: {nt}")

    nt1 = res.data["NT"]
    nt2 = res.data["NT2"]

    # Creating EdgeTypes
    res = db.create_edge_type("ET", [nt1], [nt2])
    res = db.create_edge_type("ET2", [nt1], [nt1])

    # Listing EdgeTypes
    print("Listing edge types")
    res = db.list_edge_types()
    for name, et in res.data.items():
        print(f"  - {name} [{et.id}]: {et}")

    et1 = res.data["ET"]
    et2 = res.data["ET2"]

    nt1.add_property_type("PT", ValueType.INT)
    nt1.add_property_type("PT2", ValueType.STRING)

    print("Listing node type property types")
    res = nt1.list_property_types()
    for name, pt in res.data.items():
        print(f"  - {name}: {pt}")

    pt1 = res.data["PT"]

    # Creating Network TestNet
    res = db.create_network("TestNet1")
    res = db.create_network("TestNet2")

    # Listing networks
    print("Listing networks:")
    res = db.list_networks()
    for name, net in res.data.items():
        print(f"  - {name}: {net}")

    net1 = res.data["TestNet1"]
    net2 = res.data["TestNet2"]

    # Creating node
    print("Creating nodes:")
    n1 = net1.create_node(nt1).data
    n2 = net2.create_node(nt2).data
    n3 = net1.create_node(nt1).data
    print(f"  - {n1}")
    print(f"  - {n2}")

    # Creating edge
    print("Creating edges:")
    e1 = db.create_edge(et1, n1, n2).data
    e2 = db.create_edge(et2, n1, n3).data

    # Listing nodes
    print("Listing nodes:")
    res = db.list_nodes()
    for node in res.data:
        print(f"  - {node}")

    # Listing edges
    print("Listing edges:")
    res = db.list_edges()
    for edge in res.data:
        print(f"  - {edge}")

    # Listing nodes from network
    print("Listing nodes from network:")
    res = net1.list_nodes()
    for node in res.data:
        print(f"  - {node}")

    # Listing edges from network
    print("Listing edges from network:")
    res = net1.list_edges()
    for edge in res.data:
        print(f"  - {edge}")

    # Listing node property types
    print("Listing node property types:")
    res = nt1.list_property_types()
    for name, p in res.data.items():
        print(f"  - {name}: {p}")

    # Listing node properties
    print("Listing node properties:")
    res = n1.add_property(pt1, 5)
    res = n1.list_properties()
    for name, p in res.data.items():
        print(f"  - {name}: {p} = {p.value}")

