import turingdb

GRAPH_NAME = "testgraph"
EXPECTED_PERSON_COUNT = 3
EXPECTED_COMPANY_COUNT = 2
EXPECTED_SKILL_COUNT = 3
EXPECTED_KNOWS_COUNT = 2
EXPECTED_WORKS_AT_COUNT = 3
EXPECTED_HAS_SKILL_COUNT = 4

def main() -> None:
    client = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')
    print("Connected to DB")

    # Load and set the graph that was created by the read command
    client.query(f"LOAD GRAPH {GRAPH_NAME}")
    print(f"Loaded graph {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    print(f"Set graph {GRAPH_NAME}")

    # Verify Person node count
    result = client.query("MATCH (n:Person) RETURN COUNT(n) as count")
    count = int(result["count"][0])
    assert count == EXPECTED_PERSON_COUNT, f"Expected {EXPECTED_PERSON_COUNT} Person nodes, got {count}"
    print(f"Person count: {count} (expected {EXPECTED_PERSON_COUNT})")

    # Verify Company node count
    result = client.query("MATCH (n:Company) RETURN COUNT(n) as count")
    count = int(result["count"][0])
    assert count == EXPECTED_COMPANY_COUNT, f"Expected {EXPECTED_COMPANY_COUNT} Company nodes, got {count}"
    print(f"Company count: {count} (expected {EXPECTED_COMPANY_COUNT})")

    # Verify Skill node count
    result = client.query("MATCH (n:Skill) RETURN COUNT(n) as count")
    count = int(result["count"][0])
    assert count == EXPECTED_SKILL_COUNT, f"Expected {EXPECTED_SKILL_COUNT} Skill nodes, got {count}"
    print(f"Skill count: {count} (expected {EXPECTED_SKILL_COUNT})")

    # Verify KNOWS edge count
    result = client.query("MATCH ()-[r:KNOWS]->() RETURN COUNT(r) as count")
    count = int(result["count"][0])
    assert count == EXPECTED_KNOWS_COUNT, f"Expected {EXPECTED_KNOWS_COUNT} KNOWS edges, got {count}"
    print(f"KNOWS edge count: {count} (expected {EXPECTED_KNOWS_COUNT})")

    # Verify WORKS_AT edge count
    result = client.query("MATCH ()-[r:WORKS_AT]->() RETURN COUNT(r) as count")
    count = int(result["count"][0])
    assert count == EXPECTED_WORKS_AT_COUNT, f"Expected {EXPECTED_WORKS_AT_COUNT} WORKS_AT edges, got {count}"
    print(f"WORKS_AT edge count: {count} (expected {EXPECTED_WORKS_AT_COUNT})")

    # Verify HAS_SKILL edge count
    result = client.query("MATCH ()-[r:HAS_SKILL]->() RETURN COUNT(r) as count")
    count = int(result["count"][0])
    assert count == EXPECTED_HAS_SKILL_COUNT, f"Expected {EXPECTED_HAS_SKILL_COUNT} HAS_SKILL edges, got {count}"
    print(f"HAS_SKILL edge count: {count} (expected {EXPECTED_HAS_SKILL_COUNT})")

    # Verify Person nodes exist with correct properties
    result = client.query("MATCH (n:Person) RETURN n.name as name, n.age as age")
    names = set(result["name"].tolist())
    ages = set(int(a) for a in result["age"].tolist())

    expected_names = {"Alice", "Bob", "Charlie"}
    expected_ages = {30, 25, 35}
    assert names == expected_names, f"Expected names {expected_names}, got {names}"
    assert ages == expected_ages, f"Expected ages {expected_ages}, got {ages}"
    print(f"Person nodes verified: names={names}, ages={ages}")

    # Verify Company nodes exist with correct properties
    result = client.query("MATCH (n:Company) RETURN n.name as name, n.founded as founded")
    names = set(result["name"].tolist())
    founded = set(int(f) for f in result["founded"].tolist())

    expected_names = {"Acme Corp", "Globex Inc"}
    expected_founded = {1990, 2005}
    assert names == expected_names, f"Expected company names {expected_names}, got {names}"
    assert founded == expected_founded, f"Expected founded {expected_founded}, got {founded}"
    print(f"Company nodes verified: names={names}, founded={founded}")

    # Verify Skill nodes exist with correct properties
    result = client.query("MATCH (n:Skill) RETURN n.name as name, n.level as level")
    names = set(result["name"].tolist())
    levels = set(result["level"].tolist())

    expected_names = {"Python", "Java", "Rust"}
    expected_levels = {"expert", "intermediate", "beginner"}
    assert names == expected_names, f"Expected skill names {expected_names}, got {names}"
    assert levels == expected_levels, f"Expected levels {expected_levels}, got {levels}"
    print(f"Skill nodes verified: names={names}, levels={levels}")

    # Verify KNOWS edge properties
    result = client.query("""
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        RETURN a.name as from_name, b.name as to_name, r.since as since
    """)
    from_names = set(result["from_name"].tolist())
    to_names = set(result["to_name"].tolist())
    since_values = set(int(s) for s in result["since"].tolist())

    assert from_names == {"Alice", "Bob"}, f"Expected from_names {{'Alice', 'Bob'}}, got {from_names}"
    assert to_names == {"Bob", "Charlie"}, f"Expected to_names {{'Bob', 'Charlie'}}, got {to_names}"
    assert since_values == {2020, 2021}, f"Expected since {{2020, 2021}}, got {since_values}"
    print(f"KNOWS edges verified: from={from_names}, to={to_names}, since={since_values}")

    # Verify WORKS_AT edge properties
    result = client.query("""
        MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
        RETURN p.name as person, c.name as company, r.since as since
    """)
    persons = set(result["person"].tolist())
    companies = set(result["company"].tolist())
    since_values = set(int(s) for s in result["since"].tolist())

    assert persons == {"Alice", "Bob", "Charlie"}, f"Expected persons {{'Alice', 'Bob', 'Charlie'}}, got {persons}"
    assert companies == {"Acme Corp", "Globex Inc"}, f"Expected companies {{'Acme Corp', 'Globex Inc'}}, got {companies}"
    assert since_values == {2018, 2019, 2020}, f"Expected since {{2018, 2019, 2020}}, got {since_values}"
    print(f"WORKS_AT edges verified: persons={persons}, companies={companies}, since={since_values}")

    # Verify HAS_SKILL edges
    result = client.query("""
        MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
        RETURN p.name as person, s.name as skill
    """)
    person_skills = set(zip(result["person"].tolist(), result["skill"].tolist()))

    expected_skills = {("Alice", "Python"), ("Alice", "Java"), ("Bob", "Python"), ("Charlie", "Rust")}
    assert person_skills == expected_skills, f"Expected person_skills {expected_skills}, got {person_skills}"
    print(f"HAS_SKILL edges verified: {person_skills}")

    print("* read_script: all assertions passed")

if __name__ == "__main__":
    main()
