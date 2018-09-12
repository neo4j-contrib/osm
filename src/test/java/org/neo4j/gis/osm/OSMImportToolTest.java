package org.neo4j.gis.osm;

import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.map;

public class OSMImportToolTest {

    @Test
    public void testOneStreet() {
        importAndAssert("one-street", (db, stats) -> {
            stats.put("expectedOSMNodes", 8L);
            stats.put("expectedOSMWayNodes", 8L);
            stats.put("expectedOSMWays", 1L);
            assertOSMModel(db, stats);
        });
    }

    @Test
    public void testOneWayStreet() {
        importAndAssert("one-way-forward", (db, stats) -> {
            stats.put("expectedOSMNodes", 4L);
            stats.put("expectedOSMWayNodes", 4L);
            stats.put("expectedOSMWays", 1L);
            assertOSMModel(db, stats);
            assertOSMWay(db, Direction.OUTGOING, 72090582, new long[]{857081476, 857081950, 857081819, 857081796});
        });
    }

    @Test
    public void testOneWayStreetBackwards() {
        importAndAssert("one-way-backward", (db, stats) -> {
            stats.put("expectedOSMNodes", 4L);
            stats.put("expectedOSMWayNodes", 4L);
            stats.put("expectedOSMWays", 1L);
            assertOSMModel(db, stats);
            assertOSMWay(db, Direction.INCOMING, 72090582, new long[]{857081476, 857081950, 857081819, 857081796});
        });
    }

    @Test
    public void testTwoStreet() {
        importAndAssert("two-street", (db, stats) -> {
            stats.put("expectedOSMNodes", 24L);
            stats.put("expectedOSMWayNodes", 24L);
            stats.put("expectedOSMWays", 2L);
            assertOSMModel(db, stats);
        });
    }

    @Test
    public void testParking() {
        importAndAssert("parking", (db, stats) -> {
            stats.put("expectedOSMNodes", 4L);
            stats.put("expectedOSMWayNodes", 4L);
            stats.put("expectedOSMWays", 1L);
            assertOSMModel(db, stats);
        });
    }

    @Test
    public void testParkingAndStreets() {
        importAndAssert("parking-and-streets", (db, stats) -> {
            stats.put("expectedOSMNodes", 17L);
            stats.put("expectedOSMWayNodes", 26L);
            stats.put("expectedOSMWays", 7L);
            stats.put("expectedOSMRelations", 1L);
            stats.put("expectedOSMRelationMembers", 3L);
            assertOSMModel(db, stats);
        });
    }

    @Test
    public void testOSM() {
        importAndAssert("map", (db, stats) -> {
            stats.put("expectedOSMNodes", 2334L);
            stats.put("nodesWithTags", 202L);
            stats.put("expectedOSMWayNodes", 2588L - stats.get("closedWays"));
            stats.put("expectedOSMWays", 167L);
            stats.put("expectedOSMRelations", 6L);
            stats.put("expectedOSMRelationMembers", 40L); // 424 are defined, but only 40 exist in same file
            assertOSMModel(db, stats);
        });
    }

    @Test
    public void testOSM2() {
        importAndAssert("map2", (db, stats) -> {
            stats.put("expectedOSMTags", 8796L);
            stats.put("expectedOSMNodes", 43630L);
            stats.put("expectedOSMWayNodes", 50703L);
            stats.put("expectedOSMWays", 7023L);
            stats.put("expectedOSMRelations", 115L);
            stats.put("expectedOSMRelationMembers", 626L);
            stats.put("expectedNextRels", 46903L);
            assertOSMModel(db, stats);
        });
    }

    @Test
    public void testMultiOSM() {
        importAndAssert("test", new String[]{"map", "map2"}, (db, stats) -> {
            stats.put("expectedOSMTags", 9170L);
            stats.put("expectedOSMNodes", 45964L);
            stats.put("expectedOSMWayNodes", 53273L);
            stats.put("expectedOSMWays", 7190L);
            stats.put("expectedOSMRelations", 120L);
            assertOSMModel(db, stats, true);    // merging OSM models currently duplicates relationships
        });
    }

    private File prepareStoreDir(String name) throws IOException {
        File storeDir = new File("target/databases/", name);
        FileUtils.deleteRecursively(storeDir);
        storeDir.mkdirs();
        return storeDir;
    }

    private File findOSMFile(String name) {
        for (String ext : new String[]{".osm.bz2", ".osm"}) {
            File file = new File("samples/" + name + ext);
            if (file.exists()) return file;
        }
        return null;
    }

    private void importAndAssert(String name, BiConsumer<GraphDatabaseService, Map<String, Long>> assertions) {
        File osmFile = findOSMFile(name);
        if (osmFile != null) {
            try {
                File storeDir = prepareStoreDir(name);
                OSMImportTool.main(new String[]{"--into", storeDir.getCanonicalPath(), osmFile.getCanonicalPath()});
                System.out.println("\nFinished importing " + osmFile + " - analysing database ...");
                GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
                Map<String, Long> stats = debugOSMModel(db);
                assertions.accept(db, stats);
                db.shutdown();
            } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace(System.err);
                throw new RuntimeException(e);
            }
        }
    }

    private void importAndAssert(String name, String[] files, BiConsumer<GraphDatabaseService, Map<String, Long>> assertions) {
        String[] args = new String[files.length + 3];
        for (int i = 0; i < files.length; i++) {
            try {
                args[i + 3] = findOSMFile(files[i]).getCanonicalPath();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            File storeDir = prepareStoreDir(name);
            args[0] = "--skip-duplicate-nodes";
            args[1] = "--into";
            args[2] = storeDir.getCanonicalPath();
            OSMImportTool.main(args);
            System.out.println("\nFinished importing " + Arrays.toString(files) + " into " + storeDir + " - analysing database ...");
            GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
            Map<String, Long> stats = debugOSMModel(db);
            assertions.accept(db, stats);
            db.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        if (result.hasNext()) {
            System.out.println(format("%8s%8s", "ways", "count"));
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                System.out.println(format("%8d%8d", Long.parseLong(record.get("ways").toString()), Long.parseLong(record.get("count").toString())));
            }
        }
        return stats;
    }

    private void assertOSMModel(GraphDatabaseService db, Map<String, Long> stats) {
        assertOSMModel(db, stats, false);
    }

    private void assertOSMModel(GraphDatabaseService db, Map<String, Long> stats, boolean ignoreRelationships) {
        long expectedOSMNodes = getFromStats(stats, "expectedOSMNodes", 0);
        long expectedOSMWayNodes = getFromStats(stats, "expectedOSMWayNodes", expectedOSMNodes);
        long expectedOSMWays = getFromStats(stats, "expectedOSMWays", 0);
        long expectedOSMRelations = getFromStats(stats, "expectedOSMRelations", 0);
        long expectedOSMRelationMembers = getFromStats(stats, "expectedOSMRelationMembers", 0);
        long nodesWithTags = getFromStats(stats, "nodesWithTags", 0);
        long closedWays = getFromStats(stats, "closedWays", 0);
        long expectedOSMTags = getFromStats(stats, "expectedOSMTags", nodesWithTags + expectedOSMWays + expectedOSMRelations);
        long expectedNextRels = getFromStats(stats, "expectedNextRels", expectedOSMWayNodes - expectedOSMWays + closedWays);
        map(
                "OSMNode", expectedOSMNodes,
                "OSMWay", expectedOSMWays,
                "OSMRelation", expectedOSMRelations,
                "OSMWayNode", expectedOSMWayNodes,
                "OSMTags", expectedOSMTags
        ).forEach(
                (label, count) -> assertThat("Expected specific number of '" + label + "' nodes", countNodesWithLabel(db, label), equalTo(count))
        );
        if (!ignoreRelationships) {
            map(
                    "TAGS", expectedOSMTags,
                    "FIRST_NODE", expectedOSMWays,
                    "NEXT", expectedNextRels,
                    "NODE", expectedOSMWayNodes,
                    "MEMBER", expectedOSMRelationMembers
            ).forEach(
                    (type, count) -> assertThat("Expected specific number of '" + type + "' relationships", countRelationshipsWithType(db, type), equalTo(count))
            );
        }
    }

    private void assertOneWay(Node node, Direction direction, String[] forwardValues, String[] backwardValues) {
        Map<String, Object> properties = node.getAllProperties();
        assertThat("Expected one-way tag", properties, hasKey("oneway"));
        String oneway = properties.get("oneway").toString();
        if (direction == Direction.OUTGOING) {
            assertThat("Expected forward direction oneway to be valid entry", oneway, isOneOf(forwardValues));
        } else if (direction == Direction.INCOMING) {
            assertThat("Expected reverse direction oneway to be '-1'", oneway, isOneOf(backwardValues));
        }

    }

    private String[] a(String... fields) {
        return fields;
    }

    private void assertOSMWay(GraphDatabaseService db, Direction direction, int wayId, long[] expectedNodeIds) {
        try (Transaction tx = db.beginTx()) {
            Node way = db.findNode(Label.label("OSMWay"), "way_osm_id", wayId);
            if (way != null) {
                assertOneWay(way, direction, a("FORWARD"), a("BACKWARD"));
                if (way.hasRelationship(RelationshipType.withName("TAGS"), Direction.OUTGOING)) {
                    Node tags = way.getSingleRelationship(RelationshipType.withName("TAGS"), Direction.OUTGOING).getEndNode();
                    assertOneWay(tags, direction, a("yes", "true", "1"), a("-1"));
                }
                if (way.hasRelationship(RelationshipType.withName("FIRST_NODE"), Direction.OUTGOING)) {
                    Node firstNode = way.getSingleRelationship(RelationshipType.withName("FIRST_NODE"), Direction.OUTGOING).getEndNode();
                    ArrayList<Node> wayNodes = new ArrayList<>(expectedNodeIds.length);
                    for (Path path : db.traversalDescription().depthFirst().relationships(RelationshipType.withName("NEXT"), direction).traverse(firstNode)) {
                        Node endNode = path.endNode();
                        if (wayNodes.size() > 0 && endNode.getId() == firstNode.getId()) {
                            break;
                        }
                        wayNodes.add(endNode);
                    }
                    long[] nodeIds = new long[wayNodes.size()];
                    int wayNodeIndex = 0;
                    for (Node wayNode : wayNodes) {
                        Node node = wayNode.getSingleRelationship(RelationshipType.withName("NODE"), Direction.OUTGOING).getEndNode();
                        nodeIds[wayNodeIndex] = (Long) node.getProperty("node_osm_id");
                        wayNodeIndex++;
                    }
                    assertThat("Should have correct node ID order", nodeIds, equalTo(expectedNodeIds));
                } else {
                    fail("Way has no outgoing FIRST_NODE relationship");
                }
            } else {
                fail("No way found with way_osm_id = " + wayId);
            }
            tx.success();
        }
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
