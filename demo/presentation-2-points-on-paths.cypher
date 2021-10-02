//
// Find the nodes that are on streets and mark as OSMPathNodes
// ? Did we really use this in the demo, I think not - double check this
//

// Mark OSMNodes on streets as OSMPathNodes to improve performance of other queries

PROFILE MATCH (wt:OSMTags)
  WHERE exists(wt.highway)
MATCH (wt)<-[:TAGS]-(w:OSMWay)-[:FIRST_NODE]->(n:OSMWayNode)
WITH n LIMIT 10
MATCH (n)-[:NEXT*0..]->(x:OSMWayNode)
WITH x
MATCH (x)-[:NODE]->(o:OSMNode)
  WHERE NOT o:OSMPathNode
SET o:OSMPathNode
RETURN count(o);

// Using periodic iterate

CALL apoc.periodic.iterate(
'MATCH (wt:OSMTags)
   WHERE exists(wt.highway)
 MATCH (wt)<-[:TAGS]-(w:OSMWay)-[:FIRST_NODE]->(n:OSMWayNode)
 RETURN n',
'MATCH (n)-[:NEXT*0..]->(x:OSMWayNode)
 WITH x
 MATCH (x)-[:NODE]->(o:OSMNode)
   WHERE NOT o:OSMPathNode
   SET o:OSMPathNode
 RETURN count(o)',
{batchSize:1000, parallel:false});

// On NewYork dataset it took 130s to mark 800k OSMNodes as OSMPathNodes
// This was only 10% of all OSMNodes, so clearly much more efficient to work with these than all nodes
// On SanFrancisco dataset it took 9s to mark 49548 OSMNodes as OSMPathNodes (8%)
// On Sweden 2020 it took 269s to mark 14680965 (14M) OSMPathNodes
// On Sweden 2021 it took 532s to mark 16038570 (16M) OSMPathNodes
// On Australia 2021 it took 329s to mark 21009183 (21M) OSMPathNodes

CREATE INDEX ON :OSMPathNode(location);
CREATE INDEX ON :OSMNode(location);
