// Q13. Single shortest path
/*
:param [{ person1Id, person2Id }] => { RETURN
  8796093022390 AS person1Id,
  8796093022357 AS person2Id
}
*/
MATCH
    (person1:Person {id: $person1Id}),
    (person2:Person {id: $person2Id}),
    path = shortestPath((person1)-[:KNOWS*]-(person2))
RETURN
    CASE path IS NULL
        WHEN true THEN -1
        ELSE length(path)
    END AS shortestPathLength
