package org.neo4j.gis.osm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class OSMModelIntegrationTest {

    private GraphDatabaseService db;
    private TestOSMModel osm;

    @Before
    public void setup() {
        this.db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        this.osm = new TestOSMModel(db);
        this.osm.build();
    }

    @After
    public void shutdown() {
        this.db.shutdown();
    }

    @Test
    public void shouldFindClosest() {
        assertFindWay("Left", 2, 112000, 1, 2);
        assertFindWay("Top", 2, 112000, 2, 9);
        assertFindWay("Right", 2, 112000, 9, 2);
        assertFindWay("Bottom", 8, 112000, 8, 1);
    }

    @Test
    public void shouldFindClosestFurther() {
        assertFindWay("Bottom", 5, 112000, 5, 1);
        assertFindWay("Bottom", 5, 223000, 5, 2);
        assertFindWay("Bottom", 5, 334000, 5, 3);
        assertFindWay("Bottom", 5, 446000, 5, 4);
    }

    @Test
    public void shouldFindClosestCloser() {
        assertFindWay("Bottom", 5, 112000, 5, 1);
        assertFindWay("Bottom", 5, 56000, 5, 0.5);
        assertFindWay("Bottom", 5, 11200, 5, 0.1);
    }

    @Test
    public void shouldInterpolateNewNode() {
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 112000, 4.5, 1);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 56000, 4.5, 0.5);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 11200, 4.5, 0.1);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 1120, 4.5, 0.01);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 112, 4.5, 0.001);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 112, 4.5, -0.001);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 1120, 4.5, -0.01);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 11200, 4.5, -0.1);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 56000, 4.5, -0.5);
        assertFindWayAndInterpolatedPoint("Bottom", new int[]{4, 5}, 112000, 4.5, -1);
    }

    @Test
    public void shouldInterpolateNewNode2() {
        assertFindWayAndInterpolatedPoint("Right", new int[]{7, 8}, 221000, 8, 7.5);
        assertFindWayAndInterpolatedPoint("Right", new int[]{7, 8}, 112000, 9, 7.5);
        assertFindWayAndInterpolatedPoint("Right", new int[]{7, 8}, 10, 10, 7.5);
        assertFindWayAndInterpolatedPoint("Right", new int[]{7, 8}, 112000, 11, 7.5);
        assertFindWayAndInterpolatedPoint("Right", new int[]{7, 8}, 221000, 12, 7.5);
    }

    private void assertFindWay(String name, int expectedNode, double maxDist, double... coords) {
        OSMModel.OSMWay expectedWay = osm.getWay(name);
        OSMModel.LocatedNode poi = osm.makeNode(coords);
        OSMModel.OSMWay closest = osm.ways.stream().min(osm.closestWay(poi)).orElseGet(() -> null);
        assertThat("Found way with wrong name", closest.getName(), equalTo(name));
        assertThat("Found wrong way", closest, equalTo(expectedWay));
        OSMModel.OSMWay.DistanceResult distanceResult = closest.getClosest();
        assertThat("Distance to found node is too long", distanceResult.distance, lessThan(maxDist));
        assertThat("Not the closest node expected from index " + expectedNode, distanceResult.closestNodeIndex, equalTo(expectedNode));
        OSMModel.LocationMaker location = distanceResult.getLocationMaker();
        assertThat(location, instanceOf(OSMModel.LocationExists.class));
        OSMModel.LocatedNode expectedClosestNode = expectedWay.nodes.get(expectedNode);
        assertThat("Should find the expected node", ((OSMModel.LocationExists) location).node, equalTo(expectedClosestNode.node));
    }

    private void assertFindWayAndInterpolatedPoint(String name, int[] expectedPair, double maxDist, double... coords) {
        OSMModel.OSMWay expectedWay = osm.getWay(name);
        OSMModel.LocatedNode poi = osm.makeNode(coords);
        OSMModel.OSMWay closest = osm.ways.stream().min(osm.closestWay(poi)).orElseGet(() -> null);
        assertThat("Found way with wrong name", closest.getName(), equalTo(name));
        assertThat("Found wrong way", closest, equalTo(expectedWay));
        OSMModel.OSMWay.DistanceResult distanceResult = closest.getClosest();
        OSMModel.LocatedNode left = expectedWay.nodes.get(expectedPair[0]);
        OSMModel.LocatedNode right = expectedWay.nodes.get(expectedPair[1]);
        OSMModel.LocationMaker location = distanceResult.getLocationMaker();
        OSMModel.Triangle triangle;
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = location.process(db);
            PointValue point = (PointValue) node.getProperty("location");
            triangle = new OSMModel.Triangle(point, left.point, right.point);
            tx.success();
        }
        Node leftNode = null;
        Node rightNode = null;
        if (location instanceof OSMModel.LocationInterpolated) {
            OSMModel.LocationInterpolated interpolated = (OSMModel.LocationInterpolated) location;
            leftNode = interpolated.left;
            rightNode = interpolated.right;
            try (Transaction tx = db.beginTx()) {
                Node connected = poi.getNode().getSingleRelationship(OSMModel.ROUTE, Direction.OUTGOING).getEndNode();
                assertThat("Should be connected to new node", connected, equalTo(node));
                tx.success();
            }
        } else if (location instanceof OSMModel.LocationIsPoint) {
            OSMModel.LocationIsPoint interpolated = (OSMModel.LocationIsPoint) location;
            leftNode = interpolated.left;
            rightNode = interpolated.right;
        } else {
            fail("Unknown location type: " + location.getClass().getSimpleName());
        }
        assertThat("Projected point should be on the line between the two original points", triangle.apexAngle(), closeTo(180.0, 5.0));
        assertThat("Should find the expected left node", leftNode, equalTo(left.node));
        assertThat("Should find the expected right node", rightNode, equalTo(right.node));
        double distance = distanceResult.calculator.distance(triangle.apex, poi.point);
        assertThat("Distance to interpolated node is too long", distance, lessThan(maxDist));
        assertConnectedNodes(node, leftNode, rightNode);
    }

    private void assertConnectedNodes(Node node, Node left, Node right) {
        try (Transaction tx = db.beginTx()) {
            HashSet<Node> nodes = new HashSet<>();
            for (Relationship rel : node.getRelationships(OSMModel.ROUTE, Direction.OUTGOING)) {
                nodes.add(rel.getEndNode());
            }
            assertThat("Should be connected to two nodes", nodes.size(), equalTo(2));
            assertThat("Should contain left and right node", nodes, contains(left, right));
            tx.success();
        }
    }

    public static class TestOSMModel extends OSMModel {
        public ArrayList<OSMModel.OSMWay> ways;
        public HashMap<PointValue, OSMModel.LocatedNode> nodes;

        public TestOSMModel(GraphDatabaseService db) {
            super(db);
            this.nodes = new HashMap<>();
            this.ways = new ArrayList<>();
        }

        public void build() {
            ways.add(makeHorizontalWay("Top", 10));
            ways.add(makeHorizontalWay("Bottom", 0));
            ways.add(makeVerticalWay("Left", 0));
            ways.add(makeVerticalWay("Right", 10));
        }

        public OSMModel.OSMWay getWay(String name) {
            for (OSMModel.OSMWay way : ways) {
                if (way.getName().equals(name)) return way;
            }
            return null;
        }

        public OSMModel.OSMWay makeHorizontalWay(String name, double y) {
            OSMModel.LocatedNode[] nodes = new OSMModel.LocatedNode[10];
            for (int x = 0; x < 10; x++) {
                nodes[x] = makeNode(x, y);
            }
            return makeWay(name, nodes);
        }

        public OSMModel.OSMWay makeVerticalWay(String name, double x) {
            OSMModel.LocatedNode[] nodes = new OSMModel.LocatedNode[10];
            for (int y = 0; y < 10; y++) {
                nodes[y] = makeNode(x, y);
            }
            return makeWay(name, nodes);
        }

        public OSMModel.OSMWay makeWay(String name, OSMModel.LocatedNode... nodes) {
            OSMModel.OSMWay way;
            try (Transaction tx = db.beginTx()) {
                Node wayNode = db.createNode(OSMModel.OSMWay);
                wayNode.setProperty("name", name);
                Node tags = db.createNode(OSMModel.OSMTags);
                tags.setProperty("name", name);
                tags.setProperty("highway", "residential");
                wayNode.createRelationshipTo(tags, OSMModel.TAGS);
                Node previous = null;
                for (OSMModel.LocatedNode node : nodes) {
                    Node proxy = db.createNode(OSMModel.OSMWayNode);
                    proxy.createRelationshipTo(node.node, OSMModel.NODE);
                    if (previous == null) {
                        wayNode.createRelationshipTo(proxy, OSMModel.FIRST_NODE);
                    } else {
                        previous.createRelationshipTo(proxy, OSMModel.NEXT);
                    }
                    previous = proxy;
                }
                way = this.way(wayNode);
                tx.success();
            }
            return way;
        }

        public OSMModel.LocatedNode makeNode(double... coords) {
            PointValue point = Values.pointValue(CoordinateReferenceSystem.WGS84, coords);
            OSMModel.LocatedNode located = nodes.get(point);
            if (located == null) {
                try (Transaction tx = db.beginTx()) {
                    Node node = db.createNode(OSMModel.Routable);
                    node.setProperty("location", point);
                    located = this.located(node);
                    nodes.put(point, located);
                    tx.success();
                }
            }
            return located;
        }
    }
}
