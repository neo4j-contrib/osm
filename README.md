# OSM for Neo4j

An OpenStreetMap data model and importer for Neo4j.

There exists an OSM data model and importer in the 'Neo4j Spatial' plugin project that supports
Neo4j 1.x, 2.x and 3.x. However that tool has a few concerns:

* It does not scale. The use of a lucene index for the OSM-id to Neo4j node-id mapping for creating
  relationships puts a ceiling on the effective data size loadable. Typically users only load cities
  or at most very small countries. Large countries are very hard to load, and the entire planet completely
  out of reach.
* It is entirely unrelated to the new spatial index built into Neo4j 3.4. It was designed to work exclusively
  with the RTree index and GeometryEncoder GIS interface of the _Neo4j Spatial_ project.
* It was designed to reflect the actual OSM graph model which supports a single editable graph of all data.
  This makes it a very complex model which is not suitable for some use cases like routing.

There is interest in getting a good OSM data model for many use cases
(sandbox, existing OSM users of Neo4j Spatial, etc.) and this leads us to want to create a new model
and importer with the following characteristics:

* Fast and scalable (using the parallel batch importer introduced in Neo4j 2.x)
* Possible to use without 'Neo4j Spatial' (ie. can be used on Neo4j 3.4 built-in spatial index).
* Can replace the older OSMImporter in Neo4j Spatial (ie. should work with Neo4j Spatial also).
* Support for a wider range of use cases, including routing

## Building

This is a maven project so it can be built using:

    mvn clean install

This will run all tests which involves importing some OSM files. If you want to skip that:

    mvn clean install -DskipTests

The build will produce two jars and copy them to the local maven repository:

* `target/osm-0.2.3-neo4j-4.2.3.jar` is aimed to be used as a dependency in maven projects that depend on this library
* `target/osm-0.2.3-neo4j-4.2.3-procedures.jar` including procedures, this jar can be copied directly into a Neo4j installation's `plugins` folder

We plan to make a third jar `target/osm-0.2.3-neo4j-4.2.3-all.jar` including all dependencies to faciliate running the command-line importer.
But until then you need to copy and reference all dependencies as described below.

## Running

Get all dependencies together:

    mvn dependency:copy-dependencies

To run with the jar at `target/osm-0.2.3-neo4j-4.2.3.jar`:

    java -Xms1280m -Xmx1280m \
      -cp "target/osm-0.2.3-neo4j-4.2.3.jar:target/dependency/*" org.neo4j.gis.osm.OSMImportTool \
      --skip-duplicate-nodes --delete --into target/neo4j --database map2 samples/map2.osm.bz2

This will import the `samples/map2.osm.bz2` file into the database at `target/neo4j/data/databases/map2`.
You can pass more than one file on the command-line to import multiple files into the same database.

The values you pass to the JVM memory settings should be based on the needs of the files being imported.
For very large files, use a high fraction of available machine memory. The example values above `-Xms1280m -Xmx1280m`
were sufficient to import all of Scandinavia: Sweden, Finland, Iceland, Norway and Denmark, which combined had BZ2 files of 1.5GB.

The entire US North-East has a BZ2 file of about 1.2G and so should import with similar settings.

## Changes specific to Neo4j 4.0

The above command has changed since the Neo4j 3.5 release. The `--into` argument now describes the root directory
of the Neo4j installation that contains `data/databases` and `data/transactions` subdirectories. The full path
to the database itself will be that root, with `data/databases/` and the database name appended. It is also worth noting
that the database transaction logs will be in `data/transactions/` with the database name appended. If you need to copy
the database into a separate Neo4j installation, you should copy both directories. For example:

    cp -a target/neo4j/data/databases/map2 $NEO4J_HOME/data/databases/
    cp -a target/neo4j/data/transactions/map2 $NEO4J_HOME/data/transactions/

If you are running an Enterprise version of Neo4j, you will need to run `CREATE DATABASE map2` to mount this new database into the new multi-database server.
This will _mount_ the database into the server without requiring a server restart.
See the Neo4j documentation for more details on how to run the new administration commands.
At the time of writing, the appropriate section could be found at https://neo4j.com/docs/cypher-manual/current/administration.

If you are running the community version of Neo4j, you will need to edit the neo4j.conf file to change the
value of `dbms.default_database` to the new database name and restart the server.
As was the case in Neo4j 3.x versions, the community server allows you to have as many databases as you wish, but you can only run one of them at a time.
The one exception to this is the new `system` database, which is not relevant to this spatial library
except in that it is the reason for the small changes in directory structure described above,
which necessitated the change in command-line options for the `OSMImportTool` utility.

## Other command-line options

There are many available command-line options inherited from the `neo4j-import` tool on which this was based.
Run with the `--help` option to see the complete list.

Common options are:

```
--into <home-dir>
	The root of the DBMS into which to do the import.
--database <database-name>
	Database name to import into. Must not contain existing database.
--delete <true/false>
	Whether or not to delete the existing database before creating a new one. 
	Default value: false
--range <minx,miny,maxx,maxy>
	Optional filter for including only points within the specified range
--skip-duplicate-nodes <true/false>
	Whether or not to skip importing nodes that have the same id/group. In the event 
	of multiple nodes within the same group having the same id, the first 
	encountered will be imported whereas consecutive such nodes will be skipped. 
	Skipped nodes will be logged, containing at most number of entities specified by 
	bad-tolerance, unless otherwise specified by skip-bad-entries-loggingoption. 
	Default value: false
```

## Procedures

To help build graphs that can be used for routing, two procedures have been added:

* `spatial.osm.routeIntersection(node,false,false,false)`
* `spatial.osm.routePointOfInterest(node,ways)`

These can be installed into an installation of Neo4j by copying the `osm-0.2.3-neo4j-4.2.3-procedures.jar` file into the `plugins` folder, and restarting the database.

### Creating a routing graph of intersections

First identify nodes that are interections where a traveller can make a choice:

    MATCH (n:OSMNode)
      WHERE size((n)<-[:NODE]-(:OSMWayNode)-[:NEXT]-(:OSMWayNode)) > 2
      AND NOT (n:Intersection)
    WITH n LIMIT 100
    MATCH (n)<-[:NODE]-(wn:OSMWayNode), (wn)<-[:NEXT*0..100]-(wx),
          (wx)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
      WHERE exists(wt.highway)
    SET n:Intersection
    RETURN count(*);

Then create a routing graph of `:ROUTE` relationships between the `:Intersection` nodes:

    MATCH (x:Intersection) WITH x LIMIT 100
      CALL spatial.osm.routeIntersection(x,false,false,false)
      YIELD fromNode, toNode, fromRel, toRel, distance, length, count
    WITH fromNode, toNode, fromRel, toRel, distance, length, count
    MERGE (fromNode)-[r:ROUTE {fromRel:id(fromRel),toRel:id(toRel)}]->(toNode)
      ON CREATE SET r.distance = distance, r.length = length, r.count = count
    RETURN count(*);

### Find points of interest and add to the routing graph

Using a selection of tags appropriate for your app, find nodes that are points of interest and connect them to the graph:

```
UNWIND ["restaurant","fast_food","cafe","bar","pub","ice_cream","cinema"] AS amenity
MATCH (x:OSMNode)-[:TAGS]->(t:OSMTags)
  WHERE t.amenity = amenity AND NOT (x)-[:ROUTE]->()
WITH x, x.location as poi LIMIT 100
MATCH (n:OSMNode)
  WHERE distance(poi, n.location) < 100
WITH x, n
MATCH (n)<-[:NODE]-(wn:OSMWayNode), (wn)<-[:NEXT*0..10]-(wx),
      (wx)<-[:FIRST_NODE]-(w:OSMWay)-[:TAGS]->(wt:OSMTags)
WITH x, w, wt
  WHERE exists(wt.highway)
WITH x, collect(w) as ways
  CALL spatial.osm.routePointOfInterest(x,ways) YIELD node
  SET x:PointOfInterest
RETURN count(node);
```

Link the points of interest sub-graph into the routing sub-graph:

    MATCH (x:Routable:OSMNode)
      WHERE NOT (x)-[:ROUTE]->(:Intersection) WITH x LIMIT 100
    CALL spatial.osm.routeIntersection(x,true,false,false)
      YIELD fromNode, toNode, fromRel, toRel, distance, length, count
    WITH fromNode, toNode, fromRel, toRel, distance, length, count
    MERGE (fromNode)-[r:ROUTE {fromRel:id(fromRel),toRel:id(toRel)}]->(toNode)
      ON CREATE SET r.distance = distance, r.length = length, r.count = count
    RETURN count(*);

