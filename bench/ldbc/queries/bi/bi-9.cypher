// Q9. Top thread initiators
/*
:params { startDate: datetime('2011-10-01'), endDate: datetime('2011-10-15') }
*/
MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..]-(reply:Message)
WHERE  post.creationDate >= $startDate
  AND  post.creationDate <= $endDate
  AND reply.creationDate >= $startDate
  AND reply.creationDate <= $endDate
RETURN
  person.id,
  person.firstName,
  person.lastName,
  count(DISTINCT post) AS threadCount,
  count(DISTINCT reply) AS messageCount
ORDER BY
  messageCount DESC,
  person.id ASC
LIMIT 100
