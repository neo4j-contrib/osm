//
// The routing graph can be exported to CSV for the purpose of including in other apps
// We used this in the GraphConnect Hackathon 2018 for a spatial graph-app
//

// This is best run in Cypher shell with a command like
// cat cat getRoutingGraph.cypher | bin/cypher-shell -u neo4j -p abc > routingGraph.csv

MATCH (r:OSMRelation) USING INDEX r:OSMRelation(relation_osm_id)
WHERE r.relation_osm_id=8398124
WITH r.polygon as manhattan, amanzi.boundingBoxFor(r.polygon) as bbox
WITH manhattan, distance(bbox.min,bbox.max)/2.0 AS radius, point({latitude:(bbox.min.y+bbox.max.y)/2.0,longitude:(bbox.min.x+bbox.max.x)/2.0}) as center
MATCH (p:Routable) USING INDEX p:Routable(location)
WHERE distance(p.location,center) < radius
AND amanzi.withinPolygon(p.location,manhattan)
WITH p
MATCH (p)-[:ROUTE]->(x:Routable)
RETURN p.lat, p.lon, x.lat, x.lon;

// If we've exported the routing graph, it can be reloaded into another database with

LOAD CSV WITH HEADERS FROM 'routingGraph.csv' AS line
MERGE (a:Routable {location:point({latitude:line.p_lat, longitude:line.p_lon})})
MERGE (b:Routable {location:point({latitude:line.x_lat, longitude:line.x_lon})})
MERGE (a)-[:ROUTE]->(b);
