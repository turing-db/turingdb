// Q19. Interaction path between cities
// Requires the Neo4j Graph Data Science library
/*
:params { city1Id: 669, city2Id: 648 }
*/
MATCH
  (person1:Person)-[:IS_LOCATED_IN]->(city1:City {id: $city1Id}),
  (person2:Person)-[:IS_LOCATED_IN]->(city2:City {id: $city2Id})
CALL gds.shortestPath.dijkstra.stream('bi19', {
  sourceNode: person1,
  targetNode: person2,
  relationshipWeightProperty: 'weight'
})
YIELD totalCost
WITH person1.id AS person1Id, person2.id AS person2Id, totalCost AS totalWeight
ORDER BY totalWeight ASC, person1.id ASC, person2.id ASC
WITH collect({person1Id: person1Id, person2Id: person2Id, totalWeight: totalWeight}) AS results
UNWIND results AS result
WITH result.person1Id AS person1Id, result.person2Id AS person2Id, result.totalWeight AS totalWeight
WHERE totalWeight = results[0].totalWeight
RETURN person1Id, person2Id, totalWeight
ORDER BY person1Id, person2Id
