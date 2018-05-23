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
* Wider range of use cases, including routing
