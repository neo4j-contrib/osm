package org.neo4j.gis.osm.procedures;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.osm.model.OSMModel;
import org.neo4j.gis.osm.model.TestOSMModel;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

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
    private DatabaseManagementService databases;
    private GraphDatabaseService db;
    //private TestOSMModel osm;

    @Before
    public void setUp() throws KernelException {
        databases = new TestDatabaseManagementServiceBuilder().impermanent().build();
        db = databases.database("neo4j");
        registerProcedure(db, OSMProcedures.class);
        try (Transaction tx = db.beginTx()) {
            TestOSMModel osm = new TestOSMModel(tx);
            osm.buildSquare(10);
            osm.buildMultiChain("ChainTopRight", 10, 10, 10, 5, 1, 1);
            osm.buildMultiChain("ChainBottomRight", 10, 0, 10, 5, 1, -1);
            osm.buildMultiChain("ChainTopLeft", 0, 10, 10, 5, -1, 1);
            osm.buildMultiChain("ChainBottomLeft", 0, 0, 10, 5, -1, -1);
            osm.addIntersectionLabels();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            var result = tx.execute("MATCH (n) RETURN count(n)");
            result.accept((row)-> true);
            tx.commit();
        }
    }

    @After
    public void tearDown() {
        databases.shutdown();
    }

    public static void testCall(Transaction tx, String call, Consumer<Map<String, Object>> consumer) {
        testCall(tx, call, null, consumer);
    }

    private static Map<String, Object> map(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i].toString(), values[i + 1]);
        }
        return map;
    }

    private static void testCall(Transaction tx, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer) {
        testCall(tx, call, params, consumer, true);
    }

    private static void testCallFails(Transaction tx, String call, Map<String, Object> params, String error) {
        try {
            testResult(tx, call, params, (res) -> {
                while (res.hasNext()) {
                    res.next();
                }
            });
            fail("Expected an exception containing '" + error + "', but no exception was thrown");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), containsString(error));
        }
    }

    private static void testCall(Transaction tx, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer, boolean onlyOne) {
        testResult(tx, call, params, (res) -> {
            if (res.hasNext()) {
                Map<String, Object> row = res.next();
                consumer.accept(row);
            }
            if (onlyOne) {
                Assert.assertFalse(res.hasNext());
            }
        });
    }

    private static void testCallCount(Transaction tx, String call, Map<String, Object> params, int count) {
        testResult(tx, call, params, (res) -> {
            int numLeft = count;
            while (numLeft > 0) {
                assertTrue("Expected " + count + " results but found only " + (count - numLeft), res.hasNext());
                res.next();
                numLeft--;
            }
            Assert.assertFalse("Expected " + count + " results but there are more", res.hasNext());
        });
    }

    private static void testResult(Transaction tx, String call, Consumer<Result> resultConsumer) {
        testResult(tx, call, null, resultConsumer);
    }

    private static void testResult(Transaction tx, String call, Map<String, Object> params, Consumer<Result> resultConsumer) {
        Map<String, Object> p = (params == null) ? map() : params;
        resultConsumer.accept(tx.execute(call, p));
    }

    private static void registerProcedure(GraphDatabaseService db, Class<?> procedure) throws KernelException {
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(GlobalProcedures.class).registerProcedure(procedure);
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
        try (Transaction tx = db.beginTx()) {
            TestOSMModel osm = new TestOSMModel(tx);
            OSMModel.OSMWay chain0 = osm.getWay(branch + "-0");
            OSMModel.OSMWay chain5u = osm.getWay(branch + "-5u");
            OSMModel.OSMWay chain5d = osm.getWay(branch + "-5d");
            Node startNode = chain0.nodes.get(0).node();
            ArrayList<Node> found = new ArrayList<>();
            testResult(tx, "CALL spatial.osm.routeIntersection($osmNode,true,true,true) YIELD fromNode, wayNode, toNode, distance, fromRel, toRel RETURN fromNode, wayNode, toNode, distance, fromRel, toRel",
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
                            for (Relationship rel : startNode.getRelationships(Direction.OUTGOING, OSMModel.ROUTE)) {
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
            assertThat(branch + " intersection to be at end chain", found, hasItem(chain5d.nodes.get(0).node()));
            assertThat(branch + " intersection to be at end chain", found, hasItem(chain5u.nodes.get(0).node()));
            tx.commit();
        }
    }

    private void assertFindWayAndInterpolatedPoint(String name, double... coords) {
        try (Transaction tx = db.beginTx()) {
            // TODO this test does nothing with the way, and should assert on that
            TestOSMModel osm = new TestOSMModel(tx);
            OSMModel.OSMWay expectedWay = osm.getWay(name);
            assertNotNull("Should find a way with name '" + name + "'", expectedWay);
            OSMModel.LocatedNode poi = osm.makeNode(coords);
            testCall(tx, "MATCH (w:OSMWay) WITH collect(w) AS ways CALL spatial.osm.routePointOfInterest($node,ways) YIELD node RETURN node",
                    map("node", poi.node()), r -> {
                        assertThat(r.keySet(), contains("node"));
                        Node routeNode = (Node) r.get("node");
                        if (routeNode == poi.node()) {
                            // The point of interest lies on the closest way, so we have connected it within the route
                            HashSet<Node> connected = new HashSet<>();
                            for (Relationship rel : poi.node().getRelationships(Direction.OUTGOING, OSMModel.ROUTE)) {
                                Node osmNode = rel.getEndNode();
                                assertThat("Expected to have label 'Routable' but found " + osmNode.getLabels(), osmNode.hasLabel(Routable), equalTo(true));
                                connected.add(osmNode);
                            }
                            assertThat(connected.size(), equalTo(2));
                            assertNodeWithinWay(connected.iterator().next(), expectedWay);
                        } else {
                            // The point of interest is connected to a new interpolated point on the closest way
                            Node connected = poi.node().getSingleRelationship(OSMModel.ROUTE, Direction.OUTGOING).getEndNode();
                            assertThat(routeNode, equalTo(connected));
                            assertNodeWithinWay(connected, expectedWay);
                        }
                    });
            tx.commit();
        }
    }

    private void assertNodeWithinWay(Node node, OSMModel.OSMWay expectedWay){
        TraversalDescription findWayNode = new MonoDirectionalTraversalDescription().depthFirst()
                .relationships(OSMModel.ROUTE, Direction.OUTGOING)  // Routable nodes always connected outgoing to normal OSMNode
                .relationships(OSMModel.NODE, Direction.INCOMING)   // OSMNode connected INCOMING to OSMWayNode
                .relationships(OSMModel.NEXT, Direction.INCOMING)   // follow OSMWayNode INCOMING chain back to first OSMWayNode
                .relationships(OSMModel.FIRST_NODE, Direction.INCOMING);    // Finally find OSMWay from INCOMING FIRST_NODE
        for (Path p : findWayNode.traverse(node)) {
            Node last = p.endNode();
            System.out.println("Found node: " + last);
            for (Label l : last.getLabels()) {
                System.out.println("\tLabel: " + l);
            }
            for (Relationship r : last.getRelationships()) {
                System.out.println("\tRelationship: (" + r.getStartNode() + ") -[:" + r.getType() + "]-> (" + r.getEndNode() + ")");
            }
            if (last.hasLabel(OSMModel.OSMWay)) {
                assertThat("Expected the connected way to have the correct name", last.getProperty("name"), equalTo(expectedWay.getName()));
                if (last.getId() == expectedWay.wayNode.getId()) {
                    return;
                }
            }
        }
        fail("Did not find any matching way node for way '" + expectedWay.getName() + "' when searching from node: " + node);
    }
}

