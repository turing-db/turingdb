import sys
from turingdb import Turing

if __name__ == '__main__':
    turing = Turing("localhost:6666")
    db = turing.get_db("reactome")
    apoe4_node_id = 2104313

    # Find by id
    apoe4_node = db.list_nodes(ids=[apoe4_node_id])[0]
    # Find by property
    # from turingdb import Property
    # apoe4_node = db.list_nodes(property=Property("displayName", "APOE-4"))

    print(f"Found node: {apoe4_node.list_properties()['displayName']}")

    print("====IN NODES")
    for in_edge in apoe4_node.in_edges:
        source_name = in_edge.source.list_properties()["displayName"].value
        print(f"In: {in_edge.edge_type.name}, Node={source_name}")

    print("=====OUT NODES")
    for out_edge in apoe4_node.out_edges:
        target_name = out_edge.target.list_properties()["displayName"].value
        print(f"Out: {out_edge.edge_type.name}, Node={target_name}")
