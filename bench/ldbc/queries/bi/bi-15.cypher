// Q15. Weighted interaction paths
// Requires the Neo4j Graph Data Science library
/*
:params {
    person1Id: 14,
    person2Id: 16,
    startDate: datetime('2010-11-01'),
    endDate: datetime('2010-12-01')
}
*/
CALL gds.graph.drop('bi15', false)
YIELD graphName

// ----------------------------------------------------------------------------------------------------
WITH count(*) AS dummy
// ----------------------------------------------------------------------------------------------------

CALL gds.graph.project.cypher(
  'bi15',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)
      OPTIONAL MATCH (pA)<-[:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[:HAS_CREATOR]->(pB)
      OPTIONAL MATCH (m1)-[:REPLY_OF*0..]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
              WHERE forum.creationDate >= datetime({epochmillis: ' + $startDate.epochMillis + '})
                AND forum.creationDate <= datetime({epochmillis: ' + $endDate.epochMillis   + '})
      WITH pA, pB,
          sum(CASE forum IS NOT NULL
              WHEN true THEN
                  CASE (m1:Post OR m2:Post) WHEN true THEN 1.0
                  ELSE 0.5 END
              ELSE 0.0 END
          ) AS w
      RETURN
        id(pA) AS source,
        id(pB) AS target,
        1/(w+1) AS weight
  '
)
YIELD graphName

// ----------------------------------------------------------------------------------------------------
WITH count(*) AS dummy
// ----------------------------------------------------------------------------------------------------

CALL {
  MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
  CALL gds.shortestPath.dijkstra.stream('bi15', {
    sourceNode: person1,
    targetNode: person2,
    relationshipWeightProperty: 'weight'
  })
  YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
  RETURN totalCost
  UNION ALL
  RETURN -1.0 AS totalCost
}
RETURN max(totalCost) AS totalCost
