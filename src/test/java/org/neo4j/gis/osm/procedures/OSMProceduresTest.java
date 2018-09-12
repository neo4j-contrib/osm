package org.neo4j.gis.osm.procedures;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.gis.osm.OSMModel;
import org.neo4j.gis.osm.OSMModelIntegrationTest;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.gis.osm.OSMModel.Routable;

public class OSMProceduresTest {
    private GraphDatabaseService db;
    private OSMModelIntegrationTest.TestOSMModel osm;

    @Before
    public void setUp() throws KernelException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerProcedure(db, OSMProcedures.class);
        this.osm = new OSMModelIntegrationTest.TestOSMModel(db);
        this.osm.build();
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    public static void testCall(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
        testCall(db, call, null, consumer);
    }

    public static Map<String, Object> map(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i].toString(), values[i + 1]);
        }
        return map;
    }

    public static void testCall(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer) {
        testCall(db, call, params, consumer, true);
    }

    public static void testCallFails(GraphDatabaseService db, String call, Map<String, Object> params, String error) {
        try {
            testResult(db, call, params, (res) -> {
                while (res.hasNext()) {
                    res.next();
                }
            });
            fail("Expected an exception containing '" + error + "', but no exception was thrown");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), containsString(error));
        }
    }

    public static void testCall(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer, boolean onlyOne) {
        testResult(db, call, params, (res) -> {
            if (res.hasNext()) {
                Map<String, Object> row = res.next();
                consumer.accept(row);
            }
            if (onlyOne) {
                Assert.assertFalse(res.hasNext());
            }
        });
    }

    public static void testCallCount(GraphDatabaseService db, String call, Map<String, Object> params, int count) {
        testResult(db, call, params, (res) -> {
            int numLeft = count;
            while (numLeft > 0) {
                assertTrue("Expected " + count + " results but found only " + (count - numLeft), res.hasNext());
                res.next();
                numLeft--;
            }
            Assert.assertFalse("Expected " + count + " results but there are more", res.hasNext());
        });
    }

    public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
        testResult(db, call, null, resultConsumer);
    }

    public static void testResult(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? map() : params;
            resultConsumer.accept(db.execute(call, p));
            tx.success();
        }
    }

    public static void registerProcedure(GraphDatabaseService db, Class<?> procedure) throws KernelException {
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(procedure);
    }

    @Test
    public void shouldInterpolateNewNode() {
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, 1);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, 0.5);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, 0.1);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, 0.01);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, 0.001);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, -0.001);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, -0.01);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, -0.1);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, -0.5);
        assertFindWayAndInterpolatedPoint("Bottom", 4.5, -1);
    }

    private void assertFindWayAndInterpolatedPoint(String name, double... coords) {
        OSMModel.OSMWay expectedWay = osm.getWay(name);
        OSMModel.LocatedNode poi = osm.makeNode(coords);
        testCall(db, "MATCH (w:OSMWay) WITH collect(w) AS ways CALL spatial.osm.route($node,ways) YIELD node RETURN node",
                map("node", poi.node), r -> {
                    assertThat(r.keySet(), contains("node"));
                    Node routeNode = (Node) r.get("node");
                    if (routeNode == poi.node) {
                        // We have inserted a node between two OSMNodes
                        HashSet<Node> connected = new HashSet<>();
                        for (Relationship rel : poi.node.getRelationships(OSMModel.ROUTE, Direction.OUTGOING)) {
                            Node osmNode = rel.getEndNode();
                            assertThat("Expected to have label 'Routable' but found " + osmNode.getLabels(), osmNode.hasLabel(Routable), equalTo(true));
                            connected.add(osmNode);
                        }
                        assertThat(connected.size(), equalTo(2));
                    } else {
                        // We created a new connected node
                        Node connected = poi.getNode().getSingleRelationship(OSMModel.ROUTE, Direction.OUTGOING).getEndNode();
                        assertThat(routeNode, equalTo(connected));
                    }
                });
    }
}

