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
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.neo4j.helpers.collection.MapUtil.map;

public class OSMImportToolTest {

    @Test
    public void testOSM() throws IOException {
        File storeDir = new File("test");
        FileUtils.deleteRecursively(storeDir);
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new OSMImportTool("samples/map.osm", storeDir.getCanonicalPath()).run(fs);
        }
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
        long expectedOSMNodes = 2334;
        long nodesWithTags = 202;
        long expectedOSMWays = 167;
        long nonWayNodes = countResult(db, "MATCH (n:OSMNode) WHERE NOT exists((n)<--(:OSMWayNode)) RETURN count(n) AS count");
        for (String label : new String[]{"OSMNode", "OSMWay", "OSMWayNode", "OSMTags"}) {
            System.out.println("Have nodes with label '" + label + "':" + countNodesWithLabel(db, label));
        }
        for (String type : new String[]{"TAGS", "FIRST_NODE", "NEXT", "NODE"}) {
            System.out.println("Have relationships of type '" + type + "':" + countRelationshipsWithType(db, type));
        }
        System.out.println("Have unattached nodes:" + nonWayNodes);

        map(
                "OSMNode", expectedOSMNodes,
                "OSMWay", expectedOSMWays,
                "OSMWayNode", expectedOSMNodes - nonWayNodes,
                "OSMTags", expectedOSMWays + nodesWithTags
        ).forEach(
                (label, count) -> assertThat("Expected specific number of '" + label + "' nodes", countNodesWithLabel(db, label), equalTo(count))
        );
        map(
                "TAGS", expectedOSMWays + nodesWithTags
        ).forEach(
                (type, count) -> assertThat("Expected specific number of '" + type + "' relationships", countRelationshipsWithType(db, type), equalTo(count))
        );
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
