CREATE (alice:Person {name: 'Alice', age: 30}),
(bob:Person {name: 'Bob', age: 25}),
(charlie:Person {name: 'Charlie', age: 35}),
(alice)-[:KNOWS {since: 2020}]->(bob),
(bob)-[:KNOWS {since: 2021}]->(charlie)

CREATE (acme:Company {name: 'Acme Corp', founded: 1990}),
(globex:Company {name: 'Globex Inc', founded: 2005})

CREATE (python:Skill {name: 'Python', level: 'expert'}),
(java:Skill {name: 'Java', level: 'intermediate'}),
(rust:Skill {name: 'Rust', level: 'beginner'})

CREATE (alice)-[:WORKS_AT {since: 2018}]->(acme)
CREATE (bob)-[:WORKS_AT {since: 2020}]->(acme)
CREATE (charlie)-[:WORKS_AT {since: 2019}]->(globex)

CREATE (alice)-[:HAS_SKILL]->(python)
CREATE (alice)-[:HAS_SKILL]->(java)
CREATE (bob)-[:HAS_SKILL]->(python)
CREATE (charlie)-[:HAS_SKILL]->(rust)
