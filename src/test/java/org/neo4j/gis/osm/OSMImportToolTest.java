package org.neo4j.gis.osm;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.neo4j.helpers.collection.MapUtil.map;

public class OSMImportToolTest {

    private File prepareStoreDir(String name) throws IOException {
        File storeDir = new File("target/databases/", name);
        FileUtils.deleteRecursively(storeDir);
        storeDir.mkdirs();
        return storeDir;
    }

    @Test
    public void testOneStreet() throws IOException {
        File storeDir = prepareStoreDir("one-street");
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new OSMImportTool("samples/one-street.osm", storeDir.getCanonicalPath()).run(fs);
        }
        System.out.println("\nFinished importing - analysing database ...");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
        Map<String, Long> stats = debugOSMModel(db);
        stats.put("expectedOSMNodes", 8L);
        stats.put("expectedOSMWayNodes", 8L);
        stats.put("expectedOSMWays", 1L);
        assertOSMModel(db, stats);
    }

    @Test
    public void testTwoStreet() throws IOException {
        File storeDir = prepareStoreDir("two-street");
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new OSMImportTool("samples/two-street.osm", storeDir.getCanonicalPath()).run(fs);
        }
        System.out.println("\nFinished importing - analysing database ...");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
        Map<String, Long> stats = debugOSMModel(db);
        stats.put("expectedOSMNodes", 24L);
        stats.put("expectedOSMWayNodes", 24L);
        stats.put("expectedOSMWays", 2L);
        assertOSMModel(db, stats);
    }

    @Test
    public void testParking() throws IOException {
        File storeDir = prepareStoreDir("parking");
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new OSMImportTool("samples/parking.osm", storeDir.getCanonicalPath()).run(fs);
        }
        System.out.println("\nFinished importing - analysing database ...");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
        Map<String, Long> stats = debugOSMModel(db);
        stats.put("expectedOSMNodes", 4L);
        stats.put("expectedOSMWayNodes", 4L);
        stats.put("expectedOSMWays", 1L);
        assertOSMModel(db, stats);
    }

    @Test
    public void testParkingAndStreets() throws IOException {
        File storeDir = prepareStoreDir("parking-and-streets");
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new OSMImportTool("samples/parking-and-streets.osm", storeDir.getCanonicalPath()).run(fs);
        }
        System.out.println("\nFinished importing - analysing database ...");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
        Map<String, Long> stats = debugOSMModel(db);
        stats.put("expectedOSMNodes", 17L);
        stats.put("expectedOSMWayNodes", 26L);
        stats.put("expectedOSMWays", 7L);
        assertOSMModel(db, stats);
    }

    @Test
    public void testOSM() throws IOException {
        File storeDir = prepareStoreDir("map");
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new OSMImportTool("samples/map.osm", storeDir.getCanonicalPath()).run(fs);
        }
        System.out.println("\nFinished importing - analysing database ...");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
        Map<String, Long> stats = debugOSMModel(db);
        stats.put("expectedOSMNodes", 2334L);
        stats.put("nodesWithTags", 202L);
        stats.put("expectedOSMWayNodes", 2588L - stats.get("closedWays"));
        stats.put("expectedOSMWays", 167L);
        stats.put("expectedOSMTags", stats.get("expectedOSMWays") + stats.get("nodesWithTags"));
        assertOSMModel(db, stats);
    }

    private Map<String, Long> debugOSMModel(GraphDatabaseService db) {
        HashMap<String, Long> stats = new HashMap<>();
        stats.put("nonWayNodes", countResult(db, "MATCH (n:OSMNode) WHERE NOT exists((n)<--(:OSMWayNode)) RETURN count(n) AS count"));
        debugLine(stats.get("nonWayNodes"), "Nodes with label 'OSMNode' that are not part of any ways");
        stats.put("sharedWayNodes", countResult(db, "MATCH (n:OSMNode)<-[r:NODE]-() WITH n, count(r) as ways WHERE ways > 1 RETURN count(n) AS count"));
        debugLine(stats.get("sharedWayNodes"), "Nodes with label 'OSMNode' that are part of more than one way");
        stats.put("closedWays", countResult(db, "MATCH (n:OSMWayNode)<-[:FIRST_NODE]-(), ()-[:NEXT]->(n)-[:NEXT]->() RETURN count(n) AS count"));
        debugLine(stats.get("closedWays"), "Ways that are closed (have same first and end way node)");
        for (String label : new String[]{"OSMNode", "OSMWay", "OSMWayNode", "OSMTags"}) {
            debugLine(countNodesWithLabel(db, label), "Nodes with label '" + label + "'");
        }
        for (String type : new String[]{"TAGS", "FIRST_NODE", "NEXT", "NODE"}) {
            debugLine(countRelationshipsWithType(db, type), "Relationships of type '" + type + "'");
        }
        Result result = db.execute("MATCH ()-[:NODE]->(n:OSMNode)<-[:NODE]-() WITH n, count(n) AS ways RETURN ways, count(ways) AS count");
        System.out.println(format("%8s%8s", "ways", "count"));
        while (result.hasNext()) {
            Map<String, Object> record = result.next();
            System.out.println(format("%8d%8d", Long.parseLong(record.get("ways").toString()), Long.parseLong(record.get("count").toString())));
        }
        return stats;
    }

    private void assertOSMModel(GraphDatabaseService db, Map<String, Long> stats) {
        long expectedOSMNodes = getFromStats(stats, "expectedOSMNodes", 0);
        long expectedOSMWayNodes = getFromStats(stats, "expectedOSMWayNodes", expectedOSMNodes);
        long expectedOSMWays = getFromStats(stats, "expectedOSMWays", 0);
        long nodesWithTags = getFromStats(stats, "nodesWithTags", 0);
        long closedWays = getFromStats(stats, "closedWays", 0);
        map(
                "OSMNode", expectedOSMNodes,
                "OSMWay", expectedOSMWays,
                "OSMWayNode", expectedOSMWayNodes,
                "OSMTags", expectedOSMWays + nodesWithTags
        ).forEach(
                (label, count) -> assertThat("Expected specific number of '" + label + "' nodes", countNodesWithLabel(db, label), equalTo(count))
        );
        map(
                "TAGS", expectedOSMWays + nodesWithTags,
                "FIRST_NODE", expectedOSMWays,
                "NEXT", expectedOSMWayNodes - expectedOSMWays + closedWays,
                "NODE", expectedOSMWayNodes
        ).forEach(
                (type, count) -> assertThat("Expected specific number of '" + type + "' relationships", countRelationshipsWithType(db, type), equalTo(count))
        );
    }

    private long getFromStats(Map<String, Long> stats, String key, long ifNull) {
        return stats.getOrDefault(key, ifNull);
    }

    private void debugLine(long count, String message) {
        System.out.println(format("%8d\t%s", count, message));
    }

    private long countNodesWithLabel(GraphDatabaseService db, String label) {
        return countResult(db, "MATCH (n:" + label + ") RETURN count(n) AS count");
    }

    private long countRelationshipsWithType(GraphDatabaseService db, String type) {
        return countResult(db, "MATCH ()-[r:" + type + "]->() RETURN count(r) AS count");
    }

    private long countResult(GraphDatabaseService db, String query) {
        Result result = db.execute(query);
        assertThat("Expected query to return a result", result.hasNext(), equalTo(true));
        Map<String, Object> record = result.next();
        assertThat("Expected record to have a count field", record, hasKey("count"));
        return (Long) record.get("count");
    }
}
