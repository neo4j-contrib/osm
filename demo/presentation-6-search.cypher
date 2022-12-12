// Make some more indexes
// (unless already made from the indexes.txt file and presentation-0 instructions)

CREATE INDEX ON :OSMRelation(relation_osm_id);
CREATE INDEX ON :OSMWay(way_osm_id);
CREATE INDEX ON :OSMNode(node_osm_id);
CREATE INDEX ON :OSMNode(location);
CREATE INDEX ON :Intersection(location);
CREATE INDEX ON :Routable(location);
CREATE INDEX ON :PointOfInterest(location);
CREATE INDEX ON :OSMPathNode(location);
CREATE INDEX ON :OSMWay(name);
CREATE INDEX ON :PointOfInterest(name);

// Search for the Marriott Marquis

MATCH (w:OSMWay)-[:FIRST_NODE]->(n:OSMWayNode)
  WHERE w.name = 'New York Marriott Marquis'
MATCH (n)-[:NEXT*..]-(x:OSMWayNode)
RETURN w, n, x;

// Search from one poi to another

MATCH (a:PointOfInterest)
  WHERE a.name = 'The View Restaurant & Lounge'
MATCH (b:PointOfInterest)
  WHERE b.name = 'Gregory Coffee'
MATCH p=shortestPath((a)-[:ROUTE*..100]-(b))
RETURN p;


//
// Using graph-algos for routing
//

// We need to extract sub-graphs to pass to the graph-algo library, so lets label for that

MATCH (i:Intersection) WHERE NOT i:Routable SET i:Routable;

MATCH (r:Routable) WHERE exists(r.location) AND NOT exists(r.lat) SET r.lat = r.location.latitude, r.lon = r.location.longitude;

// Now we can use dikstra and A-star

//... (look in code)

