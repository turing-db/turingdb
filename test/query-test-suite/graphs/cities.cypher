CREATE (paris:City {name: 'Paris', population: 2100000}),
       (lyon:City {name: 'Lyon', population: 520000}),
       (marseille:City {name: 'Marseille', population: 870000}),
       (lille:City {name: 'Lille', population: 235000}),
       (paris)-[:ROAD {km: 465}]->(lyon),
       (lyon)-[:ROAD {km: 315}]->(marseille),
       (marseille)-[:ROAD {km: 775}]->(paris),
       (paris)-[:ROAD {km: 225}]->(lille);

MATCH (lille:City {name: 'Lille'}), (lyon:City {name: 'Lyon'})
CREATE (lille)-[:TRAIN {minutes: 180}]->(lyon);
