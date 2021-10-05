//
// Identify (:OSMNode) instances that are intersections (connected INDIRECTLY to more than one (:OSMWayNode) and on ways or relations that are also streets.
//

MATCH (n:OSMNode)
  WHERE size((n)<-[:NODE]-(:OSMWayNode)-[:NEXT]-(:OSMWayNode)) > 2
  AND NOT (n:Intersection)
WITH n LIMIT 100
MATCH (n)<-[:NODE]-(wn:OSMWayNode), (wn)<-[:NEXT*0..100]-(wx),
      (wx)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
  WHERE exists(wt.highway) AND NOT n:Intersection
SET n:Intersection
RETURN COUNT(*);

// Periodic iterate

CALL apoc.periodic.iterate(
'MATCH (n:OSMNode) WHERE NOT (n:Intersection)
 AND size((n)<-[:NODE]-(:OSMWayNode)-[:NEXT]-(:OSMWayNode)) > 2 RETURN n',
'MATCH (n)<-[:NODE]-(wn:OSMWayNode), (wn)<-[:NEXT*0..100]-(wx),
       (wx)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
   WHERE exists(wt.highway) AND NOT n:Intersection
 SET n:Intersection',
{batchSize:10000, parallel:true});

MATCH (i:OSMNode) RETURN 'OSM Nodes' AS type, count(i)
UNION
MATCH (i:OSMPathNode) RETURN 'Nodes on paths' AS type, count(i)
UNION
MATCH (i:PointOfInterest) RETURN 'Points of interest' AS type, count(i)
UNION
MATCH (i:Intersection) RETURN 'Intersections' AS type, count(i);


// Produced 50k intersections in 185s for NY
// US-NE took 45 minutes to produce 789505
// San Francisco took 16s to produce 53744 Intersections
// Sweden 2021 took 733s to produce 1949128 (2m) intersections
// Australia 2021 took 725s to produce 2463855 (2.5m) intersections
// US-South 2021 took 1219s to produce 17195001 (17m) intersections

// San Francisco
//╒════════════════════╤══════════╕
//│"type"              │"count(i)"│
//╞════════════════════╪══════════╡
//│"OSM Nodes"         │2880804   │
//├────────────────────┼──────────┤
//│"Nodes on paths"    │235730    │
//├────────────────────┼──────────┤
//│"Points of interest"│3124      │
//├────────────────────┼──────────┤
//│"Intersections"     │53744     │
//└────────────────────┴──────────┘

// Sweden
// ╒════════════════════╤══════════╕
// │"type"              │"count(i)"│
// ╞════════════════════╪══════════╡
// │"OSM Nodes"         │52292654  │
// ├────────────────────┼──────────┤
// │"Nodes on paths"    │13014665  │
// ├────────────────────┼──────────┤
// │"Points of interest"│14067     │
// ├────────────────────┼──────────┤
// │"Intersections"     │1685516   │
// └────────────────────┴──────────┘

// Sweden 2020
// ╒════════════════════╤══════════╕
// │"type"              │"count(i)"│
// ╞════════════════════╪══════════╡
// │"OSM Nodes"         │65165054  │
// ├────────────────────┼──────────┤
// │"Nodes on paths"    │14680965  │
// ├────────────────────┼──────────┤
// │"Points of interest"│99        │ - trouble setting this up (procedure does not work inside apoc.periodic.iterate on Neo4j 4.x)
// ├────────────────────┼──────────┤
// │"Intersections"     │1661040   │ - this took many tries, reducing batch size and not parallel
// └────────────────────┴──────────┘

// Sweden 2021
// ╒════════════════════╤══════════╕
// │"type"              │"count(i)"│
// ╞════════════════════╪══════════╡
// │"OSM Nodes"         │70669949  │
// ├────────────────────┼──────────┤
// │"Nodes on paths"    │16038570  │
// ├────────────────────┼──────────┤
// │"Points of interest"│15128     │ - Needed to do a periodic iterate in bash, since `spatial.osm.routePointOfInterest` does not work inside `apoc.periodic.iterate` on Neo4j 4.x
// ├────────────────────┼──────────┤
// │"Intersections"     │1949128   │
// └────────────────────┴──────────┘

// Australia 2021
// ╒════════════════════╤══════════╕
// │"type"              │"count(i)"│
// ╞════════════════════╪══════════╡
// │"OSM Nodes"         │74363900  │
// ├────────────────────┼──────────┤
// │"Nodes on paths"    │21009183  │
// ├────────────────────┼──────────┤
// │"Points of interest"│28668     │ - Needed to do a periodic iterate in bash, since `spatial.osm.routePointOfInterest` does not work inside `apoc.periodic.iterate` on Neo4j 4.x
// ├────────────────────┼──────────┤
// │"Intersections"     │2463855   │
// └────────────────────┴──────────┘

// US-South 2021
// ╒════════════════════╤══════════╕
// │"type"              │"count(i)"│
// ╞════════════════════╪══════════╡
// │"OSM Nodes"         │353623762 │
// ├────────────────────┼──────────┤
// │"Nodes on paths"    │127514561 │
// ├────────────────────┼──────────┤
// │"Points of interest"│42242     │ - This seems to have worked in osm for Neo4j 4.2.11, somehow - we should double-check in the demo
// ├────────────────────┼──────────┤
// │"Intersections"     │17195001  │
// └────────────────────┴──────────┘

// During Sweden 2019 we needed to reduce the scope to get this to work
// To limit to the areas near Malmö, Sweden
// Filter to 200km from 55.599575,13.0059854

WITH point({latitude:55.599575,longitude:13.0059854}) AS malmo
MATCH (n:OSMNode)
  WHERE size((n)<-[:NODE]-(:OSMWayNode)-[:NEXT]-(:OSMWayNode)) > 2
  AND distance(malmo, n.location) < 200000
  AND NOT (n:Intersection)
WITH n LIMIT 100
MATCH (n)<-[:NODE]-(wn:OSMWayNode), (wn)<-[:NEXT*0..100]-(wx),
      (wx)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
  WHERE exists(wt.highway) AND NOT n:Intersection
SET n:Intersection
RETURN COUNT(*);

CALL apoc.periodic.iterate(
'WITH point({latitude:55.599575,longitude:13.0059854}) AS malmo
 MATCH (n:OSMNode)
   WHERE NOT (n:Intersection)
   AND distance(malmo, n.location) < 200000
   AND size((n)<-[:NODE]-(:OSMWayNode)-[:NEXT]-(:OSMWayNode)) > 2
 RETURN n',
'MATCH (n)<-[:NODE]-(wn:OSMWayNode), (wn)<-[:NEXT*0..100]-(wx),
       (wx)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
   WHERE exists(wt.highway) AND NOT n:Intersection
 SET n:Intersection',
{batchSize:10000, parallel:true});

//
// Find and connect intersections into routes
//

MATCH (x:Intersection) WITH x LIMIT 100
  CALL spatial.osm.routeIntersection(x,true,false,false)
  YIELD fromNode, toNode, fromRel, toRel, distance, length, count
WITH fromNode, toNode, fromRel, toRel, distance, length, count
MERGE (fromNode)-[r:ROUTE {fromRel:id(fromRel),toRel:id(toRel)}]->(toNode)
  ON CREATE SET r.distance = distance, r.length = length, r.count = count
RETURN COUNT(*);

// With Periodic Iterate:

CALL apoc.periodic.iterate(
'MATCH (x:Intersection) RETURN x',
'CALL spatial.osm.routeIntersection(x,true,false,false)
   YIELD fromNode, toNode, fromRel, toRel, distance, length, count
 WITH fromNode, toNode, fromRel, toRel, distance, length, count
 MERGE (fromNode)-[r:ROUTE {fromRel:id(fromRel),toRel:id(toRel)}]->(toNode)
   ON CREATE SET r.distance = distance, r.length = length, r.count = count
 RETURN count(*)',
{batchSize:100, parallel:false});

// San Francisco took 103s to perform 54k committed operations

// If there are errors, repeat with smaller batch size to better cope with StackOverFlow

CALL apoc.periodic.iterate(
'MATCH (x:Intersection) WHERE NOT (x)-[:ROUTE]->() RETURN x',
'CALL spatial.osm.routeIntersection(x,true,false,false)
   YIELD fromNode, toNode, fromRel, toRel, distance, length, count
 WITH fromNode, toNode, fromRel, toRel, distance, length, count
 MERGE (fromNode)-[r:ROUTE {fromRel:id(fromRel),toRel:id(toRel)}]->(toNode)
   ON CREATE SET r.distance = distance, r.length = length, r.count = count
 RETURN count(*)',
{batchSize:10, parallel:false});

// Sweden 2021 took 2334s to create 6157813 (6M) relationships and set 30789065 (30M) properties
// Australia 2021 jammed part way, but finished on second try which took 3084s to create 7600156 (7M) relationships and set 38000780 (38M) properties

// Now find Routable nodes from the PointOfInterest search and link them to the route map

MATCH (x:Routable:OSMNode)
  WHERE NOT (x)-[:ROUTE]->(:Intersection) WITH x LIMIT 100
CALL spatial.osm.routeIntersection(x,true,false,false)
  YIELD fromNode, toNode, fromRel, toRel, distance, length, count
WITH fromNode, toNode, fromRel, toRel, distance, length, count
MERGE (fromNode)-[r:ROUTE {fromRel:id(fromRel),toRel:id(toRel)}]->(toNode)
  ON CREATE SET r.distance = distance, r.length = length, r.count = count
RETURN COUNT(*);

// With periodic iterate

CALL apoc.periodic.iterate(
'MATCH (x:Routable:OSMNode)
   WHERE NOT (x)-[:ROUTE]->(:Intersection) RETURN x',
'CALL spatial.osm.routeIntersection(x,true,false,false)
   YIELD fromNode, toNode, fromRel, toRel, distance, length, count
 WITH fromNode, toNode, fromRel, toRel, distance, length, count
 MERGE (fromNode)-[r:ROUTE {fromRel:id(fromRel),toRel:id(toRel)}]->(toNode)
   ON CREATE SET r.distance = distance, r.length = length, r.count = count
 RETURN count(*)',
{batchSize:10, parallel:false});

// SF took 16s to do 1538 committed operations
// Sweden 2021 took 38s to create 23656 relationships and set 118280 properties
// Australia 2021 took 28s to create 41560 relationships and set 207800 properties
// US-South 2021 took 3600s to create 48489495 relationships and set 242447475 properties

// The algorithm makes self relationships, so delete with

MATCH (a:Intersection)-[r:ROUTE]->(a) DELETE r RETURN COUNT(*);

// SF had a 402 self relationships
// Sweden 2020 had 447 self relationships
// Sweden 2021 had 11853 self relationships
// Australia 2021 had 11302 self relationships

// Now to get an idea of the distribution of route distances

MATCH (a:Intersection)-[r:ROUTE]->() RETURN 'All routes' AS type, COUNT(*) AS count
UNION
MATCH (a:Intersection)-[r:ROUTE]->() WHERE r.distance > 25 RETURN '>25m' AS type, COUNT(*) AS count
UNION
MATCH (a:Intersection)-[r:ROUTE]->() WHERE r.distance > 50 RETURN '>50m' AS type, COUNT(*) AS count
UNION
MATCH (a:Intersection)-[r:ROUTE]->() WHERE r.distance > 100 RETURN '>100m' AS type, COUNT(*) AS count
UNION
MATCH (a:Intersection)-[r:ROUTE]->() WHERE r.distance > 250 RETURN '>250m' AS type, COUNT(*) AS count
UNION
MATCH (a:Intersection)-[r:ROUTE]->() WHERE r.distance > 500 RETURN '>500m' AS type, COUNT(*) AS count
UNION
MATCH (a:Intersection)-[r:ROUTE]->() WHERE r.distance > 5000 RETURN '>5000m' AS type, COUNT(*) AS count;

// SF
//╒════════════╤═══════╕
//│"type"      │"count"│
//╞════════════╪═══════╡
//│"All routes"│86315  │
//├────────────┼───────┤
//│">25m"      │55662  │
//├────────────┼───────┤
//│">50m"      │40227  │
//├────────────┼───────┤
//│">100m"     │18992  │
//├────────────┼───────┤
//│">250m"     │3976   │
//├────────────┼───────┤
//│">500m"     │1174   │
//├────────────┼───────┤
//│">5000m"    │59     │
//└────────────┴───────┘

// Sweden 2021
// ╒════════════╤═══════╕
// │"type"      │"count"│
// ╞════════════╪═══════╡
// │"All routes"│3100157│
// ├────────────┼───────┤
// │">25m"      │2150644│
// ├────────────┼───────┤
// │">50m"      │1608239│
// ├────────────┼───────┤
// │">100m"     │1044631│
// ├────────────┼───────┤
// │">250m"     │526099 │
// ├────────────┼───────┤
// │">500m"     │283214 │
// ├────────────┼───────┤
// │">5000m"    │11468  │
// └────────────┴───────┘

// Australia 2021
// ╒════════════╤═══════╕
// │"type"      │"count"│
// ╞════════════╪═══════╡
// │"All routes"│3831167│
// ├────────────┼───────┤
// │">25m"      │2684558│
// ├────────────┼───────┤
// │">50m"      │2160446│
// ├────────────┼───────┤
// │">100m"     │1437373│
// ├────────────┼───────┤
// │">250m"     │707262 │
// ├────────────┼───────┤
// │">500m"     │441207 │
// ├────────────┼───────┤
// │">5000m"    │56076  │
// └────────────┴───────┘

// To improve inner-city routing we can optionally remove some of the longer ones which might be falsely detected

MATCH (a:Intersection)-[r:ROUTE]->() WHERE r.distance > 500 DELETE r RETURN COUNT(*);

// Did not run this on Sweden 2021 or Australia 2021
