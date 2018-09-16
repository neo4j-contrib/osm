package org.neo4j.gis.osm.procedures;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.gis.osm.model.OSMModel;
import org.neo4j.gis.osm.model.TestOSMModel;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.neo4j.gis.osm.model.OSMModel.Routable;

public class OSMProceduresTest {
    private GraphDatabaseService db;
    private TestOSMModel osm;

    @Before
    public void setUp() throws KernelException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerProcedure(db, OSMProcedures.class);
        this.osm = new TestOSMModel(db);
        this.osm.buildSquare(10);
        this.osm.buildMultiChain("ChainTopRight", 10, 10, 10, 5, 1, 1);
        this.osm.buildMultiChain("ChainBottomRight", 10, 0, 10, 5, 1, -1);
        this.osm.buildMultiChain("ChainTopLeft", 0, 10, 10, 5, -1, 1);
        this.osm.buildMultiChain("ChainBottomLeft", 0, 0, 10, 5, -1, -1);
        this.osm.addIntersectionLabels();
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    public static void testCall(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
        testCall(db, call, null, consumer);
    }

    private static Map<String, Object> map(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i].toString(), values[i + 1]);
        }
        return map;
    }

    private static void testCall(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer) {
        testCall(db, call, params, consumer, true);
    }

    private static void testCallFails(GraphDatabaseService db, String call, Map<String, Object> params, String error) {
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

    private static void testCall(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer, boolean onlyOne) {
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

    private static void testCallCount(GraphDatabaseService db, String call, Map<String, Object> params, int count) {
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

    private static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
        testResult(db, call, null, resultConsumer);
    }

    private static void testResult(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? map() : params;
            resultConsumer.accept(db.execute(call, p));
            tx.success();
        }
    }

    private static void registerProcedure(GraphDatabaseService db, Class<?> procedure) throws KernelException {
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

    @Test
    public void shouldFindIntersections() {
        assertFoundIntersections("ChainTopRight", 3);
        assertFoundIntersections("ChainTopLeft", 3);
        assertFoundIntersections("ChainBottomLeft", 3);
        assertFoundIntersections("ChainBottomRight", 3);
    }

    private void assertFoundIntersections(String branch, int count) {
        OSMModel.OSMWay chain0 = osm.getWay(branch + "-0");
        OSMModel.OSMWay chain5u = osm.getWay(branch + "-5u");
        OSMModel.OSMWay chain5d = osm.getWay(branch + "-5d");
        Node startNode = chain0.nodes.get(0).node;
        ArrayList<Node> found = new ArrayList<>();
        testResult(db, "CALL spatial.osm.routeIntersection($osmNode,true,true,true) YIELD fromNode, wayNode, toNode, distance, fromRel, toRel RETURN fromNode, wayNode, toNode, distance, fromRel, toRel",
                map("osmNode", startNode), res -> {
                    while (res.hasNext()) {
                        Map<String, Object> r = res.next();
                        assertThat(branch + " should have correct keys from return", r.keySet(), containsInAnyOrder("fromNode", "wayNode", "toNode", "distance", "fromRel", "toRel"));
                        Node fromNode = (Node) r.get("fromNode");
                        Node toNode = (Node) r.get("toNode");
                        double distance = (Double) r.get("distance");
                        assertThat(branch + " should have distance", distance, greaterThan(1000000.0));
                        Relationship fromRel = (Relationship) r.get("fromRel");
                        Relationship toRel = (Relationship) r.get("toRel");
                        assertThat(branch + " route should start with defined start node", startNode, equalTo(fromNode));
                        found.add(toNode);
                        Relationship route = null;
                        for (Relationship rel : startNode.getRelationships(OSMModel.ROUTE, Direction.OUTGOING)) {
                            if (rel.getEndNode().equals(toNode)) {
                                route = rel;
                                break;
                            }
                        }
                        assertNotNull(branch + " route relationship ending at " + toNode + " should exist", route);
                        assertThat(branch + " route relationship should have found wayNode relationship id", route.getProperty("fromRel"), equalTo(fromRel.getId()));
                        assertThat(branch + " route relationship should have found wayNode relationship id", route.getProperty("toRel"), equalTo(toRel.getId()));
                    }
                });
        assertThat(found.size(), equalTo(count));
        assertThat(branch + " intersection to be at end chain", found, hasItem(chain5d.nodes.get(0).node));
        assertThat(branch + " intersection to be at end chain", found, hasItem(chain5u.nodes.get(0).node));
    }

    private void assertFindWayAndInterpolatedPoint(String name, double... coords) {
        OSMModel.OSMWay expectedWay = osm.getWay(name);
        OSMModel.LocatedNode poi = osm.makeNode(coords);
        testCall(db, "MATCH (w:OSMWay) WITH collect(w) AS ways CALL spatial.osm.routePointOfInterest($node,ways) YIELD node RETURN node",
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

