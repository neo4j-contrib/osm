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

    public TestOSMModel(GraphDatabaseService db) {
        super(db);
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
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> routable = db.findNodes(OSMModel.Routable);
            while (routable.hasNext()) {
                Node node = routable.next();
                Iterator<Relationship> routes = node.getRelationships(OSMModel.NODE, Direction.INCOMING).iterator();
                int count = 0;
                while (routes.hasNext()) {
                    Node wayNode = routes.next().getStartNode();
                    for (Relationship rel : wayNode.getRelationships(OSMModel.NEXT, Direction.BOTH)) {
                        count++;
                    }
                }
                if (count > 2) {
                    node.addLabel(OSMModel.Intersection);
                    System.out.println("Added Intersection label to " + node);
                }
            }
            tx.success();
        }
    }

    public OSMModel.OSMWay getWay(String name) {
        for (OSMModel.OSMWay way : ways) {
            if (way.getName().equals(name)) return way;
        }
        return null;
    }

    private OSMModel.OSMWay makeHorizontalWay(int size, String name, double x, double y, int dir) {
        OSMModel.LocatedNode[] nodes = new OSMModel.LocatedNode[size + 1];
        for (int i = 0; i <= size; i++) {
            nodes[i] = makeNode(x + i * dir, y);
        }
        return makeWay(name, nodes);
    }

    private OSMModel.OSMWay makeVerticalWay(int size, String name, double x, double y, int dir) {
        OSMModel.LocatedNode[] nodes = new OSMModel.LocatedNode[size + 1];
        for (int i = 0; i <= size; i++) {
            nodes[i] = makeNode(x, y + i * dir);
        }
        return makeWay(name, nodes);
    }

    private OSMModel.OSMWay makeWay(String name, OSMModel.LocatedNode... nodes) {
        OSMModel.OSMWay way;
        CRSCalculator calculator = CoordinateReferenceSystem.WGS84.getCalculator();
        try (Transaction tx = db.beginTx()) {
            Node wayNode = db.createNode(OSMModel.OSMWay);
            wayNode.setProperty("name", name);
            Node tags = db.createNode(OSMModel.OSMTags);
            tags.setProperty("name", name);
            tags.setProperty("highway", "residential");
            wayNode.createRelationshipTo(tags, OSMModel.TAGS);
            Node previous = null;
            PointValue previousPoint = null;
            for (OSMModel.LocatedNode node : nodes) {
                Node proxy = db.createNode(OSMModel.OSMWayNode);
                proxy.createRelationshipTo(node.node, OSMModel.NODE);
                if (previous == null) {
                    wayNode.createRelationshipTo(proxy, OSMModel.FIRST_NODE);
                } else {
                    Relationship next = previous.createRelationshipTo(proxy, OSMModel.NEXT);
                    next.setProperty("distance", calculator.distance(node.point, previousPoint));
                }
                previous = proxy;
                previousPoint = node.point;
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