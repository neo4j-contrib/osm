package org.neo4j.gis.osm.model;

import org.neo4j.graphdb.*;
import org.neo4j.values.storable.CRSCalculator;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class TestOSMModel extends OSMModel {
    ArrayList<OSMModel.OSMWay> ways;
    HashMap<PointValue, LocatedNode> nodes;

    private Transaction tx;

    public TestOSMModel(Transaction tx) {
        this.tx = tx;
        this.nodes = new HashMap<>();
        this.ways = new ArrayList<>();
    }

    public void buildSquare(int size) {
        ways.add(makeHorizontalWay(size, "Top", 0, size, 1));
        ways.add(makeHorizontalWay(size, "Bottom", 0, 0, 1));
        ways.add(makeVerticalWay(size, "Left", 0, 0, 1));
        ways.add(makeVerticalWay(size, "Right", size, 0, 1));
    }

    public void buildMultiChain(String name, double xbase, double ybase, int size, int count, int xdir, int ydir) {
        for (int i = 0; i < count; i++) {
            int x = xdir * size * ((i + 1) / 2);
            int y = ydir * size * (i / 2);
            if (i % 2 == 0) {
                ways.add(makeHorizontalWay(size, name + "-" + i, x + xbase, y + ybase, xdir));
            } else {
                ways.add(makeVerticalWay(size, name + "-" + i, x + xbase, y + ybase, ydir));
            }
        }
        int x = xdir * size * ((count + 1) / 2);
        int y = ydir * size * (count / 2);
        ways.add(makeVerticalWay(size, name + "-" + count + "u", x + xbase, y + ybase, 1));
        ways.add(makeVerticalWay(size, name + "-" + count + "d", x + xbase, y + ybase, -1));
    }

    public void addIntersectionLabels() {
        ResourceIterator<Node> routable = tx.findNodes(OSMModel.Routable);
        while (routable.hasNext()) {
            Node node = routable.next();
            Iterator<Relationship> routes = node.getRelationships(Direction.INCOMING, OSMModel.NODE).iterator();
            int count = 0;
            while (routes.hasNext()) {
                Node wayNode = routes.next().getStartNode();
                for (Relationship ignore : wayNode.getRelationships(Direction.BOTH, OSMModel.NEXT)) {
                    count++;
                }
            }
            if (count > 2) {
                node.addLabel(OSMModel.Intersection);
                System.out.println("Added Intersection label to " + node);
            }
        }
    }

    public OSMModel.OSMWay getWay(String name) {
        if(ways.isEmpty()) loadWays();
        for (OSMModel.OSMWay way : ways) {
            if (way.getName().equals(name)) return way;
        }
        return null;
    }

    private OSMModel.OSMWay makeHorizontalWay(int size, String name, double x, double y, int dir) {
        OSMModel.LocatedNode[] nodes = new OSMModel.LocatedNode[size + 1];
        for (int i = 0; i <= size; i++) {
            nodes[i] = makeNode( x + i * dir, y);
        }
        return makeWay(name, nodes);
    }

    private OSMModel.OSMWay makeVerticalWay(int size, String name, double x, double y, int dir) {
        OSMModel.LocatedNode[] nodes = new OSMModel.LocatedNode[size + 1];
        for (int i = 0; i <= size; i++) {
            nodes[i] = makeNode( x, y + i * dir);
        }
        return makeWay(name, nodes);
    }

    private void loadWays() {
        ways.clear();
        ResourceIterator<Node> wayNodes = tx.findNodes(OSMModel.OSMWay);
        while (wayNodes.hasNext()) {
            Node wayNode = wayNodes.next();
            String name = (String) wayNode.getProperty("name");
            if (name == null) {
                throw new IllegalStateException("Existing way is missing 'name' property: " + wayNode);
            }
            ways.add(way(wayNode));
        }
    }

    private OSMModel.OSMWay makeWay(String name, OSMModel.LocatedNode... nodes) {
        CRSCalculator calculator = CoordinateReferenceSystem.WGS_84.getCalculator();
        Node wayNode = tx.createNode(OSMModel.OSMWay);
        wayNode.setProperty("name", name);
        Node tags = tx.createNode(OSMModel.OSMTags);
        tags.setProperty("name", name);
        tags.setProperty("highway", "residential");
        wayNode.createRelationshipTo(tags, OSMModel.TAGS);
        Node previous = null;
        PointValue previousPoint = null;
        for (OSMModel.LocatedNode node : nodes) {
            Node proxy = tx.createNode(OSMModel.OSMWayNode);
            proxy.createRelationshipTo(node.node(), OSMModel.NODE);
            if (previous == null) {
                wayNode.createRelationshipTo(proxy, OSMModel.FIRST_NODE);
            } else {
                Relationship next = previous.createRelationshipTo(proxy, OSMModel.NEXT);
                next.setProperty("distance", calculator.distance(node.point(), previousPoint));
            }
            previous = proxy;
            previousPoint = node.point();
        }
        return this.way(wayNode);
    }

    public OSMModel.LocatedNode makeNode(double... coords) {
        PointValue point = Values.pointValue(CoordinateReferenceSystem.WGS_84, coords);
        OSMModel.LocatedNode located = nodes.get(point);
        if (located == null) {
            Node node = tx.createNode(OSMModel.Routable);
            node.setProperty("location", point);
            located = this.located(node);
            nodes.put(point, located);
        }
        return located;
    }
}