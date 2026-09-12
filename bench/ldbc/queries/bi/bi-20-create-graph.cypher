CALL gds.graph.project.cypher(
    'bi20',
    'MATCH (p:Person) RETURN id(p) AS id',
    'MATCH
      (personA:Person)-[:KNOWS]-(personB:Person),
      (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
    RETURN
      id(personA) AS source,
      id(personB) AS target,
      min(abs(saA.classYear - saB.classYear)) + 1 AS weight'
)
