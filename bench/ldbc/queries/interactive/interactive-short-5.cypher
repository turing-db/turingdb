// IS5. Creator of a message
/*
:param messageId: 206158431836
 */
MATCH (m:Message {id:  $messageId })-[:HAS_CREATOR]->(p:Person)
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
