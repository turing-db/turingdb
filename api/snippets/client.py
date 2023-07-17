import sys
import turingapi

if __name__ == "__main__":
    turing = turingapi.Turing("localhost:6666")

    # Checking if server is up and running
    if not turing.running:
        print("Server is not running. Exiting")
        sys.exit()

    # List available DBs
    dbs = turing.list_available_databases()
    print("Available databases:")
    for db_name in dbs:
        print(f"  - {db_name}")

    # List loaded DBs
    loaded_dbs = turing.list_loaded_databases()
    print("Loaded databases:")
    for db_name, db in loaded_dbs.items():
        print(f"  - {db_name}: {db}")

    # The recommendations db is available from the previous list_loaded_databases
    # call, but it can also be retrieved from this call:
    print("Getting dbs")
    db = turing.get_db(db_name="recommendations")
    print(f"Recommendations:  - DB Object: {db}")

    # Deleting "custom" db from previous run
    if "custom" in loaded_dbs.keys():
        loaded_dbs["custom"].unload()

    # Creating 'custom' database:
    print("Creating db 'custom':")
    db = turing.create_db(db_name="custom")

    # Uncomment to dump DB:
    # db.dump()

    # Creating NodeTypes
    print("Creating node types 'NT' and 'NT2'")
    nt1 = db.create_node_type("NT")
    nt2 = db.create_node_type("NT2")

    # Listing NodeTypes
    print("Listing node types")
    node_types = db.list_node_types()
    for name, nt in node_types.items():
        print(f"  - {name} [{nt.id}]: {nt}")

    # Creating EdgeTypes
    et1 = db.create_edge_type("ET", [nt1], [nt2])
    et2 = db.create_edge_type("ET2", [nt1], [nt1])

    # Listing EdgeTypes
    print("Listing edge types")
    edge_types = db.list_edge_types()
    for name, et in edge_types.items():
        print(f"  - {name} [{et.id}]: {et}")

    et1 = edge_types["ET"]
    et2 = edge_types["ET2"]

    pt1 = nt1.add_property_type("PT", turingapi.ValueType.INT)
    pt2 = nt1.add_property_type("PT2", turingapi.ValueType.STRING)

    print("Listing node type property types")
    property_types = nt1.list_property_types()
    for name, pt in property_types.items():
        print(f"  - {name}: {pt}")

    pt1 = property_types["PT"]

    # Creating Network TestNet
    net1 = db.create_network("TestNet1")
    net2 = db.create_network("TestNet2")

    # Listing networks
    print("Listing networks:")
    networks = db.list_networks()
    for name, net in networks.items():
        print(f"  - {name}: {net}")

    net1 = networks["TestNet1"]
    net2 = networks["TestNet2"]

    # Creating node
    print("Creating nodes:")
    n1 = net1.create_node(nt1)
    n2 = net2.create_node(nt2)
    n3 = net1.create_node(nt1)
    print(f"  - {n1}")
    print(f"  - {n2}")

    # Creating edge
    print("Creating edges:")
    e1 = db.create_edge(et1, n1, n2)
    e2 = db.create_edge(et2, n1, n3)

    # Listing nodes
    print("Listing nodes:")
    nodes = db.list_nodes()
    for node in nodes:
        print(f"  - {node}")

    # Listing nodes by id
    print("Listing nodes by id:")
    other_nodes = db.list_nodes_by_id([0, 1])
    for node in other_nodes:
        print(f"  - {node}")

    # Listing edges
    print("Listing edges:")
    edges = db.list_edges()
    for edge in edges:
        print(f"  - {edge}")

    # Listing edges by id
    print("Listing edges by id:")
    other_edges = db.list_edges_by_id([0])
    for edge in other_edges:
        print(f"  - {edge}")

    # Listing nodes from network
    print("Listing nodes from network:")
    nodes = net1.list_nodes()
    for node in nodes:
        print(f"  - {node}")

    # Listing nodes by id from network
    print("Listing nodes by id from network:")
    other_nodes = net1.list_nodes_by_id([0, 2])
    for node in other_nodes:
        print(f"  - {node}")

    # Listing edges from network
    print("Listing edges from network:")
    edges = net1.list_edges()
    for edge in edges:
        print(f"  - {edge}")

    # Listing edges by id from network
    print("Listing edges by id from network:")
    other_edges = net1.list_edges_by_id([0])
    for edge in other_edges:
        print(f"  - {edge}")

    # Listing node properties
    print("Listing node properties:")
    p1 = n1.add_property(pt1, 5)
    properties = n1.list_properties()
    for name, p in properties.items():
        print(f"  - {name}: {p} = {p.value}")

