//
// Investigate the nature of the tags to decide which are useful for the application
//

MATCH (t:OSMTags) WHERE exists(t.amenity)
RETURN t.amenity, COUNT(*) ORDER BY COUNT(*) DESC;

// This gives 154 different values, many of which have nothing to do with the apps purpose
// (328 for Sweden 2021 and 508 for Australia 2021)

// So lets filter:

MATCH (t:OSMTags) WHERE exists(t.amenity)
WITH t.amenity AS amenity, COUNT(*) AS count ORDER BY count DESC
WHERE amenity = 'cinema' OR amenity =~ '.*cafe.*' OR amenity =~ 'restaurant'
OR amenity =~ '.*food.*' OR amenity =~ '.*cream.*' OR amenity =~ '.*bar.*' OR amenity = 'pub'
OR amenity =~ '.*club.*' OR amenity =~ 'biergarten'OR amenity =~ '.*feeding.*'
RETURN amenity, count;

// For New York:
// ╒═══════════════╤═══════╕
// │"amenity"      │"count"│
// ╞═══════════════╪═══════╡
// │"restaurant"   │4797   │
// ├───────────────┼───────┤
// │"cafe"         │1468   │
// ├───────────────┼───────┤
// │"fast_food"    │1463   │
// ├───────────────┼───────┤
// │"bar"          │794    │
// ├───────────────┼───────┤
// │"pub"          │255    │
// ├───────────────┼───────┤
// │"ice_cream"    │84     │
// ├───────────────┼───────┤
// │"cinema"       │71     │
// ├───────────────┼───────┤
// │"food_court"   │14     │
// ├───────────────┼───────┤
// │"internet_cafe"│3      │
// └───────────────┴───────┘

// For San Francisco
// ╒════════════╤═══════╕
// │"amenity"   │"count"│
// ╞════════════╪═══════╡
// │"restaurant"│1965   │
// ├────────────┼───────┤
// │"cafe"      │674    │
// ├────────────┼───────┤
// │"fast_food" │340    │
// ├────────────┼───────┤
// │"bar"       │275    │
// ├────────────┼───────┤
// │"pub"       │161    │
// ├────────────┼───────┤
// │"cinema"    │24     │
// ├────────────┼───────┤
// │"ice_cream" │23     │
// ├────────────┼───────┤
// │"food_court"│5      │
// ├────────────┼───────┤
// │"barbershop"│1      │
// └────────────┴───────┘

// For Sweden
// ╒════════════════════════════════════════════════╤═══════╕
// │"amenity"                                       │"count"│
// ╞════════════════════════════════════════════════╪═══════╡
// │"restaurant"                                    │7580   │
// ├────────────────────────────────────────────────┼───────┤
// │"fast_food"                                     │3549   │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe"                                          │3384   │
// ├────────────────────────────────────────────────┼───────┤
// │"pub"                                           │584    │
// ├────────────────────────────────────────────────┼───────┤
// │"bar"                                           │287    │
// ├────────────────────────────────────────────────┼───────┤
// │"cinema"                                        │235    │
// ├────────────────────────────────────────────────┼───────┤
// │"ice_cream"                                     │98     │
// ├────────────────────────────────────────────────┼───────┤
// │"food_court"                                    │15     │
// ├────────────────────────────────────────────────┼───────┤
// │"internet_cafe"                                 │4      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe;bench"                                    │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe;restaurant;bar"                           │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"disused:cafe"                                  │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"Thai food"                                     │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe;bicycle_rental"                           │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"barbecue"                                      │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"pub;bank;atm;cafe;restaurant;post_box;pharmacy"│1      │
// ├────────────────────────────────────────────────┼───────┤
// │"food"                                          │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe / restaurant"                             │1      │
// └────────────────────────────────────────────────┴───────┘

// For Sweden 2020
// ╒════════════════════════════════════════════════╤═══════╕
// │"amenity"                                       │"count"│
// ╞════════════════════════════════════════════════╪═══════╡
// │"restaurant"                                    │8238   │
// ├────────────────────────────────────────────────┼───────┤
// │"fast_food"                                     │3706   │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe"                                          │3569   │
// ├────────────────────────────────────────────────┼───────┤
// │"pub"                                           │590    │
// ├────────────────────────────────────────────────┼───────┤
// │"bar"                                           │316    │
// ├────────────────────────────────────────────────┼───────┤
// │"cinema"                                        │250    │
// ├────────────────────────────────────────────────┼───────┤
// │"ice_cream"                                     │130    │
// ├────────────────────────────────────────────────┼───────┤
// │"nightclub"                                     │94     │
// ├────────────────────────────────────────────────┼───────┤
// │"food_court"                                    │18     │
// ├────────────────────────────────────────────────┼───────┤
// │"biergarten"                                    │6      │
// ├────────────────────────────────────────────────┼───────┤
// │"feeding_place"                                 │5      │
// ├────────────────────────────────────────────────┼───────┤
// │"stripclub"                                     │5      │
// ├────────────────────────────────────────────────┼───────┤
// │"internet_cafe"                                 │4      │
// ├────────────────────────────────────────────────┼───────┤
// │"game_feeding"                                  │2      │
// ├────────────────────────────────────────────────┼───────┤
// │"barbecue"                                      │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe / restaurant"                             │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe;bicycle_rental"                           │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe;restaurant;bar"                           │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"computer_club"                                 │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"disused:cafe"                                  │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"food"                                          │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"food_bank"                                     │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"pub;bank;atm;cafe;restaurant;post_box;pharmacy"│1      │
// ├────────────────────────────────────────────────┼───────┤
// │"restaurant;bar"                                │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"restaurant;nightclub"                          │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"swingerclub"                                   │1      │
// └────────────────────────────────────────────────┴───────┘

// Sweden 2021

// ╒════════════════════════════════════════════════╤═══════╕
// │"amenity"                                       │"count"│
// ╞════════════════════════════════════════════════╪═══════╡
// │"restaurant"                                    │8370   │
// ├────────────────────────────────────────────────┼───────┤
// │"fast_food"                                     │3739   │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe"                                          │3610   │
// ├────────────────────────────────────────────────┼───────┤
// │"pub"                                           │585    │
// ├────────────────────────────────────────────────┼───────┤
// │"bar"                                           │315    │
// ├────────────────────────────────────────────────┼───────┤
// │"cinema"                                        │252    │
// ├────────────────────────────────────────────────┼───────┤
// │"ice_cream"                                     │140    │
// ├────────────────────────────────────────────────┼───────┤
// │"nightclub"                                     │95     │
// ├────────────────────────────────────────────────┼───────┤
// │"food_court"                                    │18     │
// ├────────────────────────────────────────────────┼───────┤
// │"biergarten"                                    │6      │
// ├────────────────────────────────────────────────┼───────┤
// │"feeding_place"                                 │5      │
// ├────────────────────────────────────────────────┼───────┤
// │"stripclub"                                     │5      │
// ├────────────────────────────────────────────────┼───────┤
// │"internet_cafe"                                 │4      │
// ├────────────────────────────────────────────────┼───────┤
// │"game_feeding"                                  │2      │
// ├────────────────────────────────────────────────┼───────┤
// │"disused:cafe"                                  │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"restaurant;bar"                                │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"restaurant;nightclub"                          │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"pub;bank;atm;cafe;restaurant;post_box;pharmacy"│1      │
// ├────────────────────────────────────────────────┼───────┤
// │"barbecue"                                      │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"computer_club"                                 │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"swingerclub"                                   │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"food"                                          │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"food_bank"                                     │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe / restaurant"                             │1      │
// ├────────────────────────────────────────────────┼───────┤
// │"cafe;bicycle_rental"                           │1      │
// └────────────────────────────────────────────────┴───────┘

// Australia 2021

// ╒═══════════════════════════╤═══════╕
// │"amenity"                  │"count"│
// ╞═══════════════════════════╪═══════╡
// │"restaurant"               │11040  │
// ├───────────────────────────┼───────┤
// │"cafe"                     │9458   │
// ├───────────────────────────┼───────┤
// │"fast_food"                │7698   │
// ├───────────────────────────┼───────┤
// │"pub"                      │4348   │
// ├───────────────────────────┼───────┤
// │"bar"                      │1259   │
// ├───────────────────────────┼───────┤
// │"cinema"                   │356    │
// ├───────────────────────────┼───────┤
// │"ice_cream"                │254    │
// ├───────────────────────────┼───────┤
// │"nightclub"                │140    │
// ├───────────────────────────┼───────┤
// │"licensed_club"            │135    │
// ├───────────────────────────┼───────┤
// │"food_court"               │94     │
// ├───────────────────────────┼───────┤
// │"biergarten"               │32     │
// ├───────────────────────────┼───────┤
// │"internet_cafe"            │28     │
// ├───────────────────────────┼───────┤
// │"club"                     │17     │
// ├───────────────────────────┼───────┤
// │"clubhouse"                │10     │
// ├───────────────────────────┼───────┤
// │"stripclub"                │9      │
// ├───────────────────────────┼───────┤
// │"social_club"              │7      │
// ├───────────────────────────┼───────┤
// │"restaurant;bar"           │4      │
// ├───────────────────────────┼───────┤
// │"juice_bar"                │4      │
// ├───────────────────────────┼───────┤
// │"post_office;cafe"         │2      │
// ├───────────────────────────┼───────┤
// │"licenced_club"            │2      │
// ├───────────────────────────┼───────┤
// │"restaurant;cafe"          │2      │
// ├───────────────────────────┼───────┤
// │"barbecue"                 │2      │
// ├───────────────────────────┼───────┤
// │"club;restaurant"          │1      │
// ├───────────────────────────┼───────┤
// │"car_wash;cafe"            │1      │
// ├───────────────────────────┼───────┤
// │"bicycle_parking;cafe"     │1      │
// ├───────────────────────────┼───────┤
// │"bicycle_rental;cafe"      │1      │
// ├───────────────────────────┼───────┤
// │"bowls club"               │1      │
// ├───────────────────────────┼───────┤
// │"bowls_club"               │1      │
// ├───────────────────────────┼───────┤
// │"food_court;drinking_water"│1      │
// ├───────────────────────────┼───────┤
// │"food_hall"                │1      │
// ├───────────────────────────┼───────┤
// │"fuel;fast_food"           │1      │
// ├───────────────────────────┼───────┤
// │"health foods"             │1      │
// ├───────────────────────────┼───────┤
// │"cafe;bank"                │1      │
// ├───────────────────────────┼───────┤
// │"cafe;bar"                 │1      │
// ├───────────────────────────┼───────┤
// │"cafe;bicycle_hire"        │1      │
// ├───────────────────────────┼───────┤
// │"cafe;bicycle_rental"      │1      │
// ├───────────────────────────┼───────┤
// │"cafe;restaurant"          │1      │
// ├───────────────────────────┼───────┤
// │"cafe;restaurant;bar"      │1      │
// ├───────────────────────────┼───────┤
// │"cafe;retail"              │1      │
// ├───────────────────────────┼───────┤
// │"restaurant; cafe"         │1      │
// ├───────────────────────────┼───────┤
// │"shelter_and_barbeques"    │1      │
// ├───────────────────────────┼───────┤
// │"licensed_bar"             │1      │
// ├───────────────────────────┼───────┤
// │"liscensed_club"           │1      │
// ├───────────────────────────┼───────┤
// │"wildlife_feeding"         │1      │
// ├───────────────────────────┼───────┤
// │"yacht_club"               │1      │
// ├───────────────────────────┼───────┤
// │"feeding_place"            │1      │
// ├───────────────────────────┼───────┤
// │"sports_club"              │1      │
// ├───────────────────────────┼───────┤
// │"pub;bar"                  │1      │
// ├───────────────────────────┼───────┤
// │"bar;restaurant"           │1      │
// └───────────────────────────┴───────┘

//
// Finding Points Of Interest based on a set of tags discovered above
//

UNWIND ['restaurant', 'fast_food', 'cafe', 'pub', 'bar', 'cinema', 'ice_cream', 'nightclub', 'food_court'] AS amenity
MATCH (x:OSMNode)-[:TAGS]->(t:OSMTags)
  WHERE t.amenity = amenity AND NOT (x)-[:ROUTE]->()
WITH x, x.location AS poi LIMIT 100
MATCH (n:OSMPathNode)
  WHERE distance(poi, n.location) < 200
WITH x, n
MATCH (n)<-[:NODE]-(:OSMWayNode)<-[:NEXT*0..10]-(:OSMWayNode)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
WITH x, w, wt
  WHERE exists(wt.highway)
WITH x, collect(w) AS ways
  CALL spatial.osm.routePointOfInterest(x,ways) YIELD node
  SET x:PointOfInterest
RETURN count(node);

// With periodic iterate

CALL apoc.periodic.iterate(
'UNWIND ["restaurant", "fast_food", "cafe", "pub", "bar", "cinema", "ice_cream", "nightclub", "food_court"] AS amenity
 MATCH (x:OSMNode)-[:TAGS]->(t:OSMTags)
   WHERE t.amenity = amenity AND NOT (x)-[:ROUTE]->()
 RETURN x, x.location as poi',
'MATCH (n:OSMPathNode) WHERE distance(n.location, poi) < 200 WITH x, n
 MATCH (n)<-[:NODE]-(wn:OSMWayNode)<-[:NEXT*0..10]-(:OSMWayNode)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
 WITH x, w, wt
   WHERE exists(wt.highway)
 WITH x, collect(w) as ways
   CALL spatial.osm.routePointOfInterest(x,ways) YIELD node
   SET x:PointOfInterest
 RETURN count(node)',
{batchSize:100, parallel:false});

// Took 110 seconds for NY to generate 7688 points of interest
// With US-NE it took over 30min to produce over 25k poi
// For San Francisco it took 23s to do 3124 points of interest
// For Sweden it took 186s to do 14121 points of interest
// For Sweden 2020 it crashed

MATCH (n:PointOfInterest) RETURN count(n);

// To save time, we try limit to near Malmo

WITH point({latitude:55.599575,longitude:13.0059854}) AS malmo
UNWIND ['restaurant', 'fast_food', 'cafe', 'pub', 'bar', 'cinema', 'ice_cream', 'nightclub', 'food_court'] AS amenity
MATCH (x:OSMNode)-[:TAGS]->(t:OSMTags)
  WHERE t.amenity = amenity AND NOT (x)-[:ROUTE]->() AND distance(x.location, malmo) < 100000
WITH x, x.location AS poi LIMIT 100
MATCH (n:OSMPathNode)
  WHERE distance(poi, n.location) < 200
WITH x, n
MATCH (n)<-[:NODE]-(:OSMWayNode)<-[:NEXT*0..10]-(:OSMWayNode)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
WITH x, w, wt
  WHERE exists(wt.highway) AND NOT wt.highway STARTS WITH "primary" AND (NOT exists(wt.foot) OR NOT wt.foot = "yes")
WITH x, collect(w) AS ways
CALL spatial.osm.routePointOfInterest(x,ways) YIELD node
SET x:PointOfInterest
RETURN count(node);

// With periodic iterate

CALL apoc.periodic.iterate(
'WITH point({latitude:55.599575,longitude:13.0059854}) AS malmo
 UNWIND ["restaurant", "fast_food", "cafe", "pub", "bar", "cinema", "ice_cream", "nightclub", "food_court"] AS amenity
 MATCH (x:OSMNode)-[:TAGS]->(t:OSMTags)
   WHERE t.amenity = amenity AND NOT (x)-[:ROUTE]->() AND distance(x.location, malmo) < 100000
 RETURN x',
'MATCH (n:OSMPathNode) WHERE distance(n.location, x.location) < 200 WITH x, n
 MATCH (n)<-[:NODE]-(wn:OSMWayNode)<-[:NEXT*0..10]-(:OSMWayNode)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
 WITH x, w, wt, min(distance(n.location, x.location)) AS distance
   WHERE exists(wt.highway) AND NOT wt.highway STARTS WITH "primary" AND (NOT exists(wt.foot) OR NOT wt.foot = "yes")
 WITH x, w, distance ORDER BY distance
 WITH x, collect(w) as ways
   CALL spatial.osm.routePointOfInterest(x,ways) YIELD node
   SET x:PointOfInterest
 RETURN count(node)',
{batchSize:100, parallel:false});

// For Australia 2021 we had 10 categories (added `licensed_club because it had higher counts than food_court)
// Near Melbourne (-37.8302/144.9691)

CALL apoc.periodic.iterate(
'WITH point({latitude:-37.8302,longitude:144.9691}) AS melbourne
 UNWIND ["restaurant", "cafe", "fast_food", "pub", "bar", "cinema", "ice_cream", "nightclub", "licensed_club", "food_court"] AS amenity
 MATCH (x:OSMNode)-[:TAGS]->(t:OSMTags)
   WHERE t.amenity = amenity AND NOT (x)-[:ROUTE]->() AND distance(x.location, melbourne) < 100000
 RETURN x',
'MATCH (n:OSMPathNode) WHERE distance(n.location, x.location) < 200 WITH x, n
 MATCH (n)<-[:NODE]-(wn:OSMWayNode)<-[:NEXT*0..10]-(:OSMWayNode)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
 WITH x, w, wt, min(distance(n.location, x.location)) AS distance
   WHERE exists(wt.highway) AND NOT wt.highway STARTS WITH "primary" AND (NOT exists(wt.foot) OR NOT wt.foot = "yes")
 WITH x, w, distance ORDER BY distance
 WITH x, collect(w) as ways
   CALL spatial.osm.routePointOfInterest(x,ways) YIELD node
   SET x:PointOfInterest
 RETURN count(node)',
{batchSize:100, parallel:false});

// The routing code does not put lat/lon on interpolated nodes, and A-Star needs these

MATCH (r:Routable) WHERE exists(r.location) AND NOT exists(r.lat)
SET r.lat = r.location.latitude, r.lon = r.location.longitude
RETURN count(r);
