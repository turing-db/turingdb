### Intro

TuringDB now has a ShortestPath Processor that allows users to perform a dijkstra search from one set of nodes to another finding the shortest distance between the sets based on a certain edge weight.


### Usage 

The syntax for the shortest path processor is as follows:
```
shortestPath(sourceSetVar,targetSetVar, edgePropName, distanceReturnVar,pathReturnVar)
```


We can provide the source and target sets through patterns that come before the shortest path call:
```
turing:dijkstraMinimalExample>   match (n{name:"Ashchurch"}), (m{name:"Worcestershire Parkway"}) shortestPath(n,m,distance,dist,path) return dist,path

+-------+--------------+
| dist  | path         |
+-------+--------------+
| 11.29 | (0)-[1]->(6) |
+-------+--------------+
```

note that we cannot access the variables of any branch being used by the shortest path processor:
```
turing:dijkstraMinimalExample>   match (n{name:"Ashchurch"}), (m{name:"Worcestershire Parkway"}) shortestPath(n,m,distance,dist,path) return dist,path,n

[2026-02-12 20:14:20] [error] PLAN_ERROR: Unexpected exception: projection variable n not found in output column
```

we can access branches that aren't being used by the shortest path processor (the output will be a cartesian product):

```
turing:dijkstraMinimalExample>   match (n{name:"Ashchurch"}), (m{name:"Worcestershire Parkway"}), (p{name:"Cheltenham Spa"}) shortestPath(n,m,distance,dist,path) return dist,path,p.name

+-------+--------------+----------------+
| dist  | path         | p.name         |
+-------+--------------+----------------+
| 11.29 | (0)-[1]->(6) | Cheltenham Spa |
+-------+--------------+----------------+

```


The distance variable is the shortest path from any node in the source set to any node in the target set
```

turing:dijkstraMinimalExample>   match (n{name:"Ashchurch"}), (m{name:"Worcestershire Parkway"}) shortestPath(n,m,distance,dist,path) return dist

+-------+
| dist  |
+-------+
| 11.29 |
+-------+
```

The path variable is a string representing the shortest path (in the format ...(nodeID)-\[edgeID\]->(nodeID)...) 
```
turing:dijkstraMinimalExample>   match (n{name:"Ashchurch"}), (m{name:"Worcestershire Parkway"}) shortestPath(n,m,distance,dist,path) return path
+--------------+
| path         |
+--------------+
| (0)-[1]->(6) |
+--------------+
```

As you can see the path does match the ids
```
 match (n)-[e]->(m) return n,n.name,e,m.name,m
+---+---------------------------+----+---------------------------+---+
| n | n.name                    | e  | m.name                    | m |
+---+---------------------------+----+---------------------------+---+
| 0 | Ashchurch                 | 0  | Cheltenham Spa            | 2 |
+---+---------------------------+----+---------------------------+---+
| 0 | Ashchurch                 | 1  | Worcestershire Parkway    | 6 |
+---+---------------------------+----+---------------------------+---+
| 0 | Ashchurch                 | 2  | Worcester Shrub Hill      | 8 |
+---+---------------------------+----+---------------------------+---+
| 1 | Bromsgrove                | 3  | Cheltenham Spa            | 2 |
+---+---------------------------+----+---------------------------+---+
| 1 | Bromsgrove                | 4  | Droitwich Spa             | 3 |
+---+---------------------------+----+---------------------------+---+
| 1 | Bromsgrove                | 5  | Worcestershire Parkway    | 6 |
+---+---------------------------+----+---------------------------+---+
| 3 | Droitwich Spa             | 6  | Hartlebury                | 4 |
+---+---------------------------+----+---------------------------+---+
| 3 | Droitwich Spa             | 7  | Worcester Foregate Street | 7 |
+---+---------------------------+----+---------------------------+---+
| 3 | Droitwich Spa             | 8  | Worcester Shrub Hill      | 8 |
+---+---------------------------+----+---------------------------+---+
| 5 | Pershore                  | 9  | Worcestershire Parkway    | 6 |
+---+---------------------------+----+---------------------------+---+
| 6 | Worcestershire Parkway    | 10 | Worcester Shrub Hill      | 8 |
+---+---------------------------+----+---------------------------+---+
| 7 | Worcester Foregate Street | 11 | Worcester Shrub Hill      | 8 |
+---+---------------------------+----+---------------------------+---+
```


### Multiple Source Set Example

As you can see below we can pass multiple sources to the shortest path processor ( the same principle applies to targets):
```
turing:dijkstraMinimalExample> match (n), (m{name:"Worcestershire Parkway"}) where n.name="Bromsgrove" or n.name = "Cheltenham Spa" shortestPath(n,m,distance,dist,path) return dist,path

+------+--------------+
| dist | path         |
+------+--------------+
| 12.6 | (1)-[5]->(6) |
+------+--------------+
```
### Cypher Sample
the following cypher sample was used in the above examples
```
CREATE (asch:Station {name:"Ashchurch"}),
  (bmv:Station {name:"Bromsgrove"}),
  (cnm:Station {name:"Cheltenham Spa"}),
  (dtw:Station {name:"Droitwich Spa"}),
  (hby:Station {name:"Hartlebury"}),
  (psh:Station {name:"Pershore"}),
  (wop:Station {name:"Worcestershire Parkway"}),
  (wof:Station {name:"Worcester Foregate Street"}),
  (wos:Station {name:"Worcester Shrub Hill"})
CREATE (asch)-[:LINK {distance: 7.25}]->(cnm),
  (asch)-[:LINK {distance: 11.29}]->(wop),
  (asch)-[:LINK {distance: 14.75}]->(wos),
  (bmv)-[:LINK {distance: 31.14}]->(cnm),
  (bmv)-[:LINK {distance: 6.16}]->(dtw),
  (bmv)-[:LINK {distance: 12.6}]->(wop),
  (dtw)-[:LINK {distance: 5.64}]->(hby),
  (dtw)-[:LINK {distance: 6.03}]->(wof),
  (dtw)-[:LINK {distance: 5.76}]->(wos),
  (psh)-[:LINK {distance: 4.16}]->(wop),
  (wop)-[:LINK {distance: 3.71}]->(wos),
  (wof)-[:LINK {distance: 0.65}]->(wos)

```


<img src = "https://neo4j.com/docs/cypher-manual/current/_images/patterns-shortest-graph.svg">
