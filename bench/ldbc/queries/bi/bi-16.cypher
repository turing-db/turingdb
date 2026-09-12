// Q16. Fake news detection
// These parameters return a 'false positive' as the maxKnowsLimit is set too high.
/*
:params {
  tagA: 'Meryl_Streep',
  dateA: datetime('2012-09-16'),
  tagB: 'Hank_Williams',
  dateB: datetime('2012-05-08'),
  maxKnowsLimit: 4
}
*/
UNWIND [
    {letter: 'A', tag: $tagA, date: $dateA},
    {letter: 'B', tag: $tagB, date: $dateB}
  ] AS param
WITH param.letter AS paramLetter, param.tag AS paramTagX, param.date AS paramDateX
CALL {
  WITH paramTagX, paramDateX
  MATCH (person1:Person)<-[:HAS_CREATOR]-(message1:Message)-[:HAS_TAG]->(tag:Tag {name: paramTagX})
  WHERE date(message1.creationDate) = date(paramDateX)
  // filter out Persons with more than $maxKnowsLimit friends who created the same kind of Message
  OPTIONAL MATCH (person1)-[:KNOWS]-(person2:Person)<-[:HAS_CREATOR]-(message2:Message)-[:HAS_TAG]->(tag)
  WHERE date(message2.creationDate) = date(paramDateX)
  WITH person1, count(DISTINCT message1) AS cm, count(DISTINCT person2) AS cp2
  WHERE cp2 <= $maxKnowsLimit
  // return count
  RETURN person1, cm
}
WITH person1, collect({letter: paramLetter, messageCount: cm}) AS results
WHERE size(results) = 2
RETURN
  person1.id,
  [r IN results WHERE r.letter = 'A' | r.messageCount][0] AS messageCountA,
  [r IN results WHERE r.letter = 'B' | r.messageCount][0] AS messageCountB
ORDER BY messageCountA + messageCountB DESC, person1.id ASC
LIMIT 20
