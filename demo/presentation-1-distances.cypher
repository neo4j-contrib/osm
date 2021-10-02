//-----------------------------------------------------------------------------------------//
// Calculate distances between first 10k way-nodes
//

MATCH (awn:OSMWayNode)-[r:NEXT]-(bwn:OSMWayNode)
  WHERE NOT exists(r.distance)
WITH awn,bwn,r LIMIT 10000

MATCH (awn)-[:NODE]->(a:OSMNode), (bwn)-[:NODE]->(b:OSMNode)
  SET r.distance=distance(a.location,b.location)
RETURN count(*);

// Neo4j 3.5.9 Batch results:
// Doing the same iteratively in batches of 10k
// took 1015s for NY (and over 3h for US-NE)
// second run took 561s (less than 6min)
// took 202s for San Francisco (6356558 relationships edited)
// Took 1977s for Sweden 2019 (33 min)
// Took 15424s for Sweden 2021 (4.28h) - 159 161 076 relationships edited
// Took 15120s for Australia 2021 (4.2h) - 152 659 797 relationships edited

// Repeating for Neo4j 4.0 NewYork took 200s

CALL apoc.periodic.iterate(
'MATCH (awn:OSMWayNode)-[r:NEXT]-(bwn:OSMWayNode)
 WHERE NOT exists(r.distance) RETURN awn,bwn,r',
'MATCH (awn)-[:NODE]->(a:OSMNode), (bwn)-[:NODE]->(b:OSMNode)
 SET r.distance=distance(a.location,b.location)',
{batchSize:10000, parallel:false});

// Calculating if there are any un-calculated distances:
// NY had 19 071 964 distances (19 million)
// US-NE had 193 329 862 (193 million)
// Sweden 2019 had 115 299 426 (115 million)
// Sweden 2020 had 146 683 242 (147 million) - took 146s on a freshly started database and 117s warmed up (SSD)
// Sweden 2021 had 159 193 474 (159 million)
// Australia 2021 had 152 684 574 (152 million)

MATCH (awn:OSMWayNode)-[r:NEXT]-(bwn:OSMWayNode)
WITH CASE
  WHEN exists(r.distance) THEN 'yes'
  ELSE 'no'
END AS has_distance
RETURN has_distance, COUNT(*) AS count;

//-----------------------------------------------------------------------------------------//
// Calculate total distances for each way (chain of nodes)
//

MATCH (w:OSMWay) WHERE NOT exists(w.distance) WITH w LIMIT 100
MATCH (w)-[:FIRST_NODE]-(n:OSMWayNode)
MATCH p=(n)-[:NEXT*]->(x)
WITH w, last(collect(p)) AS path
WITH w, path,
   reduce(l=0, r IN relationships(path) | l+r.distance) AS distance
SET w.distance=distance, w.length=length(path)
RETURN COUNT(*);

// Using periodic iterate
// NY took 577s to do 1482816 ways (380 not done due to no nodes)
// US-NE took over a day to do 9659287 ways mostly due to infinite loops in the data model
// San Francisco took 126s for 333649 ways

CALL apoc.periodic.iterate(
'MATCH (w:OSMWay) WHERE NOT exists(w.distance) RETURN w',
'MATCH (w)-[:FIRST_NODE]-(n:OSMWayNode)
 MATCH p=(n)-[:NEXT*]->(x)
 WITH w, last(collect(p)) as path
 WITH w, path,
   reduce(l=0, r in relationships(path) | l+r.distance) AS distance
 SET w.distance=distance, w.length=length(path)',
{batchSize:100, parallel:true});

// The above takes too long on bigger datasets
// Consider only shorter paths
// After running for many hours on Sweden, and only completing 30k of 5.5M ways, we tried this:

CALL apoc.periodic.iterate(
'MATCH (w:OSMWay) WHERE NOT exists(w.distance) RETURN w',
'MATCH (w)-[:FIRST_NODE]-(n:OSMWayNode)
 MATCH path=(n)-[:NEXT*..100]->(x)
   WHERE NOT exists((x)-[:NEXT]->())
 WITH w, path,
   reduce(l=0, r in relationships(path) | l+r.distance) AS distance
 SET w.distance=distance, w.length=length(path)',
{batchSize:100, parallel:false});

// For Sweden 2021 this lead to only 1979194 / 3973629 ways calculated (about 50%) after 2082s (35min)
// I then tried repeating this with a longer chain length of 200 relationships which only increased to 2001306

// Calculating if there are any uncalculated distances:

MATCH (w:OSMWay)
WITH CASE
  WHEN exists(w.distance) THEN 'yes'
  ELSE 'no'
END AS has_distance
RETURN has_distance, COUNT(*) AS count;

// US-NE has 19.5m
// NY has 1 482 816 (380 of which were zero, and set separately)

//╒══════════════╤═══════╕
//│"has_distance"│"count"│
//╞══════════════╪═══════╡
//│"no"          │380    │
//├──────────────┼───────┤
//│"yes"         │1482436│
//└──────────────┴───────┘

// Sweden 2021 with path length 200 only found:
//
//╒══════════════╤═══════╕
//│"has_distance"│"count"│
//╞══════════════╪═══════╡
//│"yes"         │2001306│
//├──────────────┼───────┤
//│"no"          │3951517│
//└──────────────┴───────┘

// So we tried to focus near Malmö (centered on Burlöv)

CALL apoc.periodic.iterate(
'WITH point({latitude:55.6333385,longitude:13.0956119}) AS center
 MATCH (w:OSMWay)-[:FIRST_NODE]-(n:OSMWayNode)-[:NODE]->(x:OSMNode)
   WHERE NOT exists(w.distance)
   AND distance(x.location,center) < 100000
 RETURN w, n',
'MATCH p=(n)-[:NEXT*..500]->(x)
 WITH w, last(collect(p)) as path
 WITH w, path, last(nodes(path)) as end
   WHERE NOT exists((end)-[:NEXT]->())
 WITH w, path,
   reduce(l=0, r in relationships(path) | l+r.distance) AS distance
 SET w.distance=distance, w.length=length(path)',
{batchSize:100, parallel:true});

// And calculate how many ways near Burlöv have distances

WITH point({latitude:55.6333385,longitude:13.0956119}) AS center
MATCH (w:OSMWay)-[:FIRST_NODE]-(n:OSMWayNode)-[:NODE]->(x:OSMNode)
  WHERE distance(x.location,center) < 100000
WITH CASE
  WHEN exists(w.distance) THEN 'yes'
  ELSE 'no'
END AS has_distance
RETURN has_distance, COUNT(*) AS count;

╒══════════════╤═══════╕
│"has_distance"│"count"│
╞══════════════╪═══════╡
│"yes"         │189744 │
├──────────────┼───────┤
│"no"          │452350 │
└──────────────┴───────┘

// For Australia 2021 we re-run many times with progressively longer path lengths

CALL apoc.periodic.iterate(
'MATCH (w:OSMWay) WHERE NOT exists(w.distance) RETURN w',
'MATCH (w)-[:FIRST_NODE]-(n:OSMWayNode)
 MATCH path=(n)-[:NEXT*..2000]->(x)
   WHERE NOT exists((x)-[:NEXT]->())
 WITH w, path,
   reduce(l=0, r in relationships(path) | l+r.distance) AS distance
 SET w.distance=distance, w.length=length(path)',
{batchSize:100, parallel:false});

// We let path length 1000 finish, but after running overnight we killed path length 2000 and still had many un-calculated ways:

╒══════════════╤═══════╕
│"has_distance"│"count"│
╞══════════════╪═══════╡
│"yes"         │2496410│
├──────────────┼───────┤
│"no"          │2669077│
└──────────────┴───────┘

//-----------------------------------------------------------------------------------------//
// Finally sum the distances up the relation tree by repeatedly running to convergence:
//

MATCH (r:OSMRelation)-[:MEMBER]-(m)
  WHERE exists(m.distance) AND NOT exists(r.distance)
WITH r, sum(m.distance) AS distance SET r.distance = distance;

// US-NE yielded results:
//    Set 76102 properties
//    Set 267 properties
//    Set 13 properties

// NY
//    Set 8794 properties
//    Set 58 properties
//    Set 6 properties

// SF
//    Set 4739 properties, completed after 219 ms.
//    Set 115 properties, completed after 9 ms.
//    Set 5 properties, completed after 5 ms.

// For Australia 2021 we also checked the number of relations with distance:

MATCH (r:OSMRelation)
WITH CASE
  WHEN exists(r.distance) THEN 'yes'
  ELSE 'no'
  END AS has_distance
RETURN has_distance, COUNT(*) AS count;

╒══════════════╤═══════╕
│"has_distance"│"count"│
╞══════════════╪═══════╡
│"yes"         │115600 │
├──────────────┼───────┤
│"no"          │23568  │
└──────────────┴───────┘
