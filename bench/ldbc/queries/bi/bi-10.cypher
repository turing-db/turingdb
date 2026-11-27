// Q10. Experts in social circle
// Requires the Neo4j APOC library
/*
:params {
    personId: 30786325588624,
    country: 'China',
    tagClass: 'MusicalArtist',
    minPathDistance: 3,
    maxPathDistance: 4
}
*/
MATCH (startPerson:Person {id: $personId})
CALL apoc.path.subgraphNodes(startPerson, {
	relationshipFilter: "KNOWS",
    minLevel: 1,
    maxLevel: $minPathDistance-1
})
YIELD node
WITH startPerson, collect(DISTINCT node) AS nodesCloserThanMinPathDistance
CALL apoc.path.subgraphNodes(startPerson, {
	relationshipFilter: "KNOWS",
    minLevel: 1,
    maxLevel: $maxPathDistance
})
YIELD node
WITH nodesCloserThanMinPathDistance, collect(DISTINCT node) AS nodesCloserThanMaxPathDistance
// compute the difference of sets: nodesCloserThanMaxPathDistance - nodesCloserThanMinPathDistance
WITH [n IN nodesCloserThanMaxPathDistance WHERE NOT n IN nodesCloserThanMinPathDistance] AS expertCandidatePersons
UNWIND expertCandidatePersons AS expertCandidatePerson
MATCH
  (expertCandidatePerson)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name: $country}),
  (expertCandidatePerson)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->
  (:TagClass {name: $tagClass})
MATCH
  (message)-[:HAS_TAG]->(tag:Tag)
RETURN
  expertCandidatePerson.id,
  tag.name,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  tag.name ASC,
  expertCandidatePerson.id ASC
LIMIT 100
