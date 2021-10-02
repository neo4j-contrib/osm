//
// NOTE: Most of these instructions work for the earlier version of the osm-routing-app
// Where the polygons were all simple Point[]
// The later versions used spatial-algorithms to build sub-graphs of shells and holes

// For example Sweden 2020/2021 polygons:
UNWIND [
  4116216,54413,52834,941530,52832,54403,52826,54374,54417,54412,52824,43332835,54409,
  4473774,9691220,54391,54386,54220,3172367,54223,52825,52827,54221,54367,54222,940675
] AS osm_id
MATCH (r:OSMRelation)
  WHERE r.relation_osm_id=osm_id
CALL spatial.osm.graph.createPolygon(r)
RETURN r.name;

// Then create cached versions of the polygons as Point[] properties
UNWIND [
  4116216,54413,52834,941530,52832,54403,52826,54374,54417,54412,52824,43332835,54409,
  4473774,9691220,54391,54386,54220,3172367,54223,52825,52827,54221,54367,54222,940675
] AS osm_id
MATCH (r:OSMRelation)
  WHERE r.relation_osm_id=osm_id
CALL spatial.osm.property.createPolygon(r)
RETURN r.name;

// For example Australia 2021 polygons:
UNWIND [
  80500,2369652,2316741,2316596,2316598,2316595,2316593,2354197,2357330,2316594,2177227,
  2177207,2559345,2574988,3225677,82636,4246124,5750005,6005680,10632587,10632642,10632701,
  10632745,10660461,11381689,3336039,3336043,3337649,3337650,3337651,3337654,3339545,3339601,
  3339672,3339705,3387680,3387681,5333901,5334286,5335846,11911716,11911717,11911812,11911862
] AS osm_id
MATCH (r:OSMRelation)
  WHERE r.relation_osm_id=osm_id
CALL spatial.osm.graph.createPolygon(r)
RETURN r.name;

// Then create cached versions of the polygons as Point[] properties
UNWIND [
  80500,2369652,2316741,2316596,2316598,2316595,2316593,2354197,2357330,2316594,2177227,
  2177207,2559345,2574988,3225677,82636,4246124,5750005,6005680,10632587,10632642,10632701,
  10632745,10660461,11381689,3336039,3336043,3337649,3337650,3337651,3337654,3339545,3339601,
  3339672,3339705,3387680,3387681,5333901,5334286,5335846,11911716,11911717,11911812,11911862
] AS osm_id
MATCH (r:OSMRelation)
  WHERE r.relation_osm_id=osm_id
CALL spatial.osm.property.createPolygon(r)
RETURN r.name;

// Older point[] instructions below:
//
// Create polygon for Manhattan
//

MATCH (r:OSMRelation)-[:MEMBER]->(n:OSMWay)
  WHERE r.relation_osm_id=8398124
WITH reverse(collect(n)) as ways
UNWIND ways as n
MATCH (n)-[:FIRST_NODE]->(a:OSMWayNode)-[:NEXT*0..]->(:OSMWayNode)-[:NODE]->(x:OSMNode)
WITH collect(id(x)) as nodes
UNWIND reduce(a=[last(nodes)], x in nodes | CASE WHEN x=last(a) THEN a ELSE a+x END) as x
MATCH (n) WHERE id(n)=x
WITH collect(n.location) as polygon
MATCH (r:OSMRelation)-[:MEMBER]->(n:OSMWay)
  WHERE r.relation_osm_id=8398124
SET r.polygon=polygon
RETURN r;

//
// Search in manhattan using bounding box (currently not working)
//

MATCH (r:OSMRelation) USING INDEX SEEK r:OSMRelation(relation_osm_id)
WHERE r.relation_osm_id=8398124
WITH r.polygon as manhattan, amanzi.boundingBoxFor(r.polygon) as bbox
MATCH (p:PointOfInterest)
WHERE bbox.min < p.location < bbox.max
AND amanzi.withinPolygon(p.location,manhattan)
RETURN count(p)
MATCH (m:OSMRelation) WHERE m.name = 'Manhattan'
WITH r.polygon as manhattan, amanzi.boundingBoxFor(r.polygon) as bbox
MATCH (p:PointOfInterest)
  WHERE bbox.min < p.location < bbox.max
  AND amanzi.withinPolygon(m.polygon,p.location)
RETURN p;

//
// Search in manhattan using distance hack
// There is a bug in the index-backed bounding box search in that if the bounds are not pure literals with no
// dependencies, the index does not get used, and a slow search is used instead. This hack uses the distance
// function which seems immune to that problem (allows dependencies)
//

PROFILE MATCH (r:OSMRelation) USING INDEX SEEK r:OSMRelation(relation_osm_id)
WHERE r.relation_osm_id=8398124
WITH r.polygon as manhattan, amanzi.boundingBoxFor(r.polygon) as bbox
WITH manhattan, distance(bbox.min,bbox.max)/2.0 AS radius, point({latitude:(bbox.min.y+bbox.max.y)/2.0,longitude:(bbox.min.x+bbox.max.x)/2.0}) as center
MATCH (p:PointOfInterest) USING INDEX SEEK p:PointOfInterest(location)
WHERE distance(p.location,center) < radius
AND amanzi.withinPolygon(p.location,manhattan)
RETURN count(p)
MATCH (m:OSMRelation) WHERE m.name = 'Manhattan'
MATCH (p:PointOfInterest)
  WHERE distance(p.location,$mapCenter) < $circleRadius
  AND amanzi.withinPolygon(m.polygon,p.location)
RETURN p;

//
// Create polygon for Queens
//

// Find OSM-id

MATCH (r:OSMRelation) WHERE r.name = 'Queens County' RETURN r.relation_osm_id;
//  --> 369519

// Calculate polygon

MATCH (r:OSMRelation)-[:MEMBER]->(n:OSMWay)
  WHERE r.relation_osm_id=369519
WITH reverse(collect(n)) as ways
UNWIND ways as n
MATCH (n)-[:FIRST_NODE]->(a:OSMWayNode)-[:NEXT*0..]->(:OSMWayNode)-[:NODE]->(x:OSMNode)
WITH collect(id(x)) as nodes
UNWIND reduce(a=[last(nodes)], x in nodes | CASE WHEN x=last(a) THEN a ELSE a+x END) as x
MATCH (n) WHERE id(n)=x
WITH collect(n.location) as polygon
MATCH (r:OSMRelation)-[:MEMBER]->(n:OSMWay)
  WHERE r.relation_osm_id=369519
SET r.polygon=polygon
RETURN r;

// Borough IDs
// Manhattan:               8398124
// Queens (Queens County):  369519
// Brooklyn (Kings County): 369518
// The Bronx (Bronx County):2552450
// Statten Island (Richmond County): 962876

// Describe borough polygon sizes:

UNWIND [8398124, 369519, 369518, 2552450, 962876] AS relId
MATCH (r:OSMRelation) WHERE r.relation_osm_id = relId
RETURN relId, r.name, size(r.polygon);

// Sweden 2020

//
// Create polygon for Skåne Län
//

// Find OSM-id

MATCH (r:OSMRelation) WHERE r.name = 'Skåne län' RETURN r.relation_osm_id;
//  --> 54409

// Calculate polygon

MATCH (r:OSMRelation)-[:MEMBER]->(n:OSMWay)
WHERE r.relation_osm_id=54409
WITH reverse(collect(n)) as ways
UNWIND ways as n
MATCH (n)-[:FIRST_NODE]->(a:OSMWayNode)-[:NEXT*0..]->(:OSMWayNode)-[:NODE]->(x:OSMNode)
WITH collect(id(x)) as nodes
UNWIND reduce(a=[last(nodes)], x in nodes | CASE WHEN x=last(a) THEN a ELSE a+x END) as x
MATCH (n) WHERE id(n)=x
WITH collect(n.location) as polygon
MATCH (r:OSMRelation)-[:MEMBER]->(n:OSMWay)
WHERE r.relation_osm_id=54409
SET r.polygon=polygon
RETURN r;

