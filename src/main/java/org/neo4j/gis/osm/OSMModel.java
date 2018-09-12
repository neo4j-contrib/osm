package org.neo4j.gis.osm;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.values.storable.CRSCalculator;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class OSMModel {
    public static final RelationshipType TAGS = RelationshipType.withName("TAGS");
    public static final RelationshipType FIRST_NODE = RelationshipType.withName("FIRST_NODE");
    public static final RelationshipType NEXT = RelationshipType.withName("NEXT");
    public static final RelationshipType NODE = RelationshipType.withName("NODE");
    public static final RelationshipType ROUTE = RelationshipType.withName("ROUTE");
    public static final Label Routable = Label.label("Routable");
    public static final Label OSMWay = Label.label("OSMWay");
    public static final Label OSMWayNode = Label.label("OSMWayNode");
    public static final Label OSMNode = Label.label("OSMNode");
    public static final Label OSMTags = Label.label("OSMTags");

    GraphDatabaseService db;

    public OSMModel(GraphDatabaseService db) {
        this.db = db;
    }

    public LocatedNode located(Node node) {
        return new LocatedNode(node);
    }

    public OSMWay way(Node node) {
        return new OSMWay(node);
    }

    public ClosestWay closestWay(LocatedNode poi) {
        return new ClosestWay(poi);
    }

    public class LocatedNode {
        public Node node;
        public PointValue point;

        LocatedNode(Node node) {
            this.node = node;
            this.point = readPoint(node);
        }

        LocatedNode(Node node, PointValue point) {
            this.node = node;
            this.point = (point == null) ? readPoint(node) : point;
        }

        @Override
        public String toString() {
            return "Node[" + node.getId() + "]:" + point;
        }

        public Node getNode() {
            return node;
        }

        PointValue readPoint(Node node) {
            if (node.hasProperty("location")) {
                Object location = node.getProperty("location");
                if (location instanceof PointValue) {
                    return (PointValue) location;
                } else if (location instanceof Point) {
                    return Values.point((Point) location);
                } else {
                    throw new IllegalArgumentException("Node does not contain a 'location' property of the right type: " + node);
                }
            } else {
                throw new IllegalArgumentException("Node does not contain a 'location' property");
            }
        }
    }

    public class OSMWay {
        String name;
        Node wayNode;
        Node tagsNode;
        public ArrayList<LocatedNode> nodes;
        HashMap<Long, LocatedNode> seenNodes;
        DistanceResult closest;

        OSMWay(Node wayNode) {
            this.wayNode = wayNode;
            this.tagsNode = wayNode.getSingleRelationship(TAGS, Direction.OUTGOING).getEndNode();
            if (wayNode.hasProperty("name")) name = wayNode.getProperty("name").toString();
            else if (tagsNode.hasProperty("name")) name = tagsNode.getProperty("name").toString();
            this.nodes = new ArrayList<>();
            this.seenNodes = new HashMap<>();
            Node firstNode = wayNode.getSingleRelationship(FIRST_NODE, Direction.OUTGOING).getEndNode();
            TraversalDescription wayNodes = db.traversalDescription().breadthFirst().relationships(NEXT, Direction.OUTGOING).evaluator(Evaluators.toDepth(20));
            for (Path path : wayNodes.traverse(firstNode)) {
                Node endNode = path.endNode();
                Node osmNode = endNode.getSingleRelationship(NODE, Direction.OUTGOING).getEndNode();
                LocatedNode node;
                if (seenNodes.containsKey(osmNode.getId())) {
                    node = seenNodes.get(osmNode.getId());
                } else {
                    node = new LocatedNode(osmNode);
                    this.seenNodes.put(osmNode.getId(), node);
                }
                this.nodes.add(node);
            }
        }

        public String getName() {
            return (name == null) ? "<unknown>" : name;
        }

        @Override
        public String toString() {
            return "OSMWay[" + wayNode.getId() + "]: name:" + name + ",  length:" + nodes.size();
        }

        public DistanceResult getClosest() {
            return closest;
        }

        DistanceResult closestDistanceTo(LocatedNode node) {
            closest = new DistanceResult(node);
            for (int i = 0; i < nodes.size(); i++) {
                LocatedNode n = nodes.get(i);
                if (!n.point.getCoordinateReferenceSystem().equals(closest.crs)) {
                    throw new IllegalArgumentException("Cannot compare points of different crs: " + n.point.getCoordinateReferenceSystem() + " != " + closest.crs);
                }
                double distance = closest.calculator.distance(node.point, n.point);
                if (distance < closest.distance) {
                    closest.distance = distance;
                    closest.closestNodeIndex = i;
                }
            }
            return closest;
        }

        public class DistanceResult {
            LocatedNode node;
            double distance;
            int closestNodeIndex;
            CoordinateReferenceSystem crs;
            public CRSCalculator calculator;

            DistanceResult(LocatedNode node) {
                this.node = node;
                crs = node.point.getCoordinateReferenceSystem();
                calculator = crs.getCalculator();
                distance = Double.MAX_VALUE;
                closestNodeIndex = -1;
            }

            @Override
            public String toString() {
                Object closest = (closestNodeIndex < 0 || closestNodeIndex >= nodes.size()) ? "null" : nodes.get(closestNodeIndex);
                return "DistanceResult: distance:" + distance + " from " + node + " to " + closest;
            }

            public LocationMaker getLocationMaker() {
                if (closestNodeIndex < 0) {
                    throw new IllegalStateException("No closest node known - has closestDistanceTo(node) not been called?");
                }
                if (closestNodeIndex == 0) {
                    if (nodes.size() > 1) {
                        return getLocationMaker(0, 1);
                    } else {
                        return new LocationExists(nodes.get(0).node);
                    }
                }
                if (closestNodeIndex == nodes.size() - 1) {
                    return getLocationMaker(closestNodeIndex - 1, closestNodeIndex);
                } else {
                    double left = calculator.distance(nodes.get(closestNodeIndex - 1).point, node.point);
                    double right = calculator.distance(nodes.get(closestNodeIndex + 1).point, node.point);
                    if (left < right) {
                        return getLocationMaker(closestNodeIndex - 1, closestNodeIndex);
                    } else {
                        return getLocationMaker(closestNodeIndex, closestNodeIndex + 1);
                    }
                }
            }

            LocationMaker getLocationMaker(int leftIndex, int rightIndex) {
                LocatedNode left = nodes.get(leftIndex);
                LocatedNode right = nodes.get(rightIndex);
                Triangle triangle = new Triangle(node.point, left.point, right.point);
                if (triangle.leftAngle() > 85.0) {
                    return new LocationExists(left.node);
                } else if (triangle.rightAngle() > 85.0) {
                    return new LocationExists(right.node);
                } else if(triangle.apexAngle() > 175) {
                    return new LocationIsPoint(left.node, right.node, node.node);
                } else {
                    PointValue projected = triangle.project();
                    return new LocationInterpolated(left.node, right.node, projected, node.node);
                }
            }
        }
    }

    public interface LocationMaker {
        public abstract Node process(GraphDatabaseService db);
    }

    public static class LocationExists implements LocationMaker {
        Node node;

        private LocationExists(Node node) {
            this.node = node;
        }

        public Node process(GraphDatabaseService db) {
            try (Transaction tx = db.beginTx()) {
                node.addLabel(Routable);
                tx.success();
            }
            return node;
        }
    }

    /**
     * This class represents the case where the point of itnerest lies between two nodes, but not on the line
     * between them so we create a new node on the street (between the two nodes) and link the original node
     * to that, and that to the street nodes in the routable graph.
     */
    public static class LocationInterpolated implements LocationMaker {
        public Node left;
        public Node right;
        public PointValue point;
        public Node poi;

        private LocationInterpolated(Node left, Node right, PointValue point, Node poi) {
            this.left = left;
            this.right = right;
            this.point = point;
            this.poi = poi;
        }

        public Node process(GraphDatabaseService db) {
            Node node;
            try (Transaction tx = db.beginTx()) {
                left.addLabel(Routable);
                right.addLabel(Routable);
                node = db.createNode(Routable);
                node.setProperty("location", point);
                node.createRelationshipTo(left, ROUTE);
                node.createRelationshipTo(right, ROUTE);
                poi.createRelationshipTo(node, ROUTE);
                tx.success();
            }
            return node;
        }
    }

    /**
     * This class represents the case where the point of interest lies on the line between two street nodes
     * and so we can simply link it directly to those nodes as part of the routable graph.
     */
    public static class LocationIsPoint implements LocationMaker {
        public Node left;
        public Node right;
        public Node node;

        private LocationIsPoint(Node left, Node right, Node node) {
            this.left = left;
            this.right = right;
            this.node = node;
        }

        public Node process(GraphDatabaseService db) {
            try (Transaction tx = db.beginTx()) {
                left.addLabel(Routable);
                right.addLabel(Routable);
                node.addLabel(Routable);
                node.createRelationshipTo(left, ROUTE);
                node.createRelationshipTo(right, ROUTE);
                tx.success();
            }
            return node;
        }
    }

    public class ClosestWay implements Comparator<OSMWay> {
        LocatedNode node;

        ClosestWay(LocatedNode node) {
            this.node = node;
        }

        public int compare(OSMWay o1, OSMWay o2) {
            OSMWay.DistanceResult d1 = o1.closestDistanceTo(node);
            OSMWay.DistanceResult d2 = o2.closestDistanceTo(node);
            return Double.compare(d1.distance, d2.distance);
        }
    }

    public static class Triangle {
        public PointValue apex;
        public PointValue left;
        public PointValue right;

        public Triangle(PointValue apex, PointValue left, PointValue right) {
            this.apex = apex;
            this.left = left;
            this.right = right;
        }

        static double angleTo(PointValue origin, PointValue point) {
            double dx = point.coordinate()[0] - origin.coordinate()[0];
            double dy = point.coordinate()[1] - origin.coordinate()[1];
            double theta = 180.0 / Math.PI * Math.atan2(dy, dx);
            return theta;
        }

        static double angle(PointValue origin, PointValue a, PointValue b) {
            double thetaA = angleTo(origin, a);
            double thetaB = angleTo(origin, b);
            double angle = Math.abs(thetaA - thetaB);
            if (angle > 180.0) angle = 360.0 - angle;
            if (angle < -180.0) angle = 360.0 + angle;
            return angle;
        }

        double leftAngle() {
            return Math.abs(angle(left, apex, right));
        }

        double rightAngle() {
            return Math.abs(angle(right, apex, left));
        }

        double apexAngle() {
            return Math.abs(angle(apex, left, right));
        }

        static double dist(PointValue origin, PointValue point) {
            double dx = point.coordinate()[0] - origin.coordinate()[0];
            double dy = point.coordinate()[1] - origin.coordinate()[1];
            return Math.sqrt(dx * dx + dy * dy);
        }

        PointValue project() {
            double gap = dist(left, right);
            double d = dist(left, apex);
            double theta = Math.PI * leftAngle() / 180.0;
            double f = d * Math.cos(theta);
            double rightFactor = Math.round(1000.0 * f / gap) / 1000.0;
            double leftFactor = 1.0 - rightFactor;
            double[] coordinates = new double[apex.coordinate().length];
            for (int i = 0; i < apex.coordinate().length; i++) {
                coordinates[i] += leftFactor * left.coordinate()[i];
                coordinates[i] += rightFactor * right.coordinate()[i];
            }
            return Values.pointValue(apex.getCoordinateReferenceSystem(), coordinates);
        }
    }
}
