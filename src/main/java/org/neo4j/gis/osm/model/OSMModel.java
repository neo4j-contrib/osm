package org.neo4j.gis.osm.model;

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
    public static final Label Intersection = Label.label("Intersection");
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

    public IntersectionRoute intersectionRoute(Node osmNode, Relationship relToStartWayNode, Node startOsmWayNode) {
        return new IntersectionRoute(osmNode, relToStartWayNode, startOsmWayNode);
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
        public ArrayList<Node> wayNodes;
        public ArrayList<LocatedNode> nodes;
        HashMap<Long, LocatedNode> seenNodes;
        DistanceResult closest;

        OSMWay(Node wayNode) {
            this.wayNode = wayNode;
            this.tagsNode = wayNode.getSingleRelationship(TAGS, Direction.OUTGOING).getEndNode();
            if (wayNode.hasProperty("name")) name = wayNode.getProperty("name").toString();
            else if (tagsNode.hasProperty("name")) name = tagsNode.getProperty("name").toString();
            this.wayNodes = new ArrayList<>();
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
                this.wayNodes.add(endNode);
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
            if (closest == null) {
                throw new IllegalStateException("No closest node found - was 'closestDistance' really called?");
            }
            return closest;
        }

        public DistanceResult getClosest(LocatedNode poi) {
            if (closest == null) {
                System.out.println("No closest way set - was 'closestDistance' really called?");
                closest = closestDistanceTo(poi);
            }
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
                        return new LocationExists(node.node, nodes.get(0).node, distance);
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
                    return new LocationExists(node.node, left.node, calculator.distance(node.point, left.point));
                } else if (triangle.rightAngle() > 85.0) {
                    return new LocationExists(node.node, right.node, calculator.distance(node.point, right.point));
                } else if (triangle.apexAngle() > 175) {
                    return new LocationIsPoint(node.node,
                            left.node, calculator.distance(left.point, node.point),
                            right.node, calculator.distance(left.point, node.point));
                } else {
                    PointValue projected = triangle.project();
                    return new LocationInterpolated(calculator, node, projected, left, right);
                }
            }
        }
    }

    public interface LocationMaker {
        Node process(GraphDatabaseService db);
    }

    public static class LocationExists implements LocationMaker {
        Node node;
        Node poi;
        double distance;

        private LocationExists(Node poi, Node node, double distance) {
            this.node = node;
            this.poi = poi;
            this.distance = distance;
        }

        public Node process(GraphDatabaseService db) {
            System.out.println("\t\tConnecting existing node: " + node);
            try (Transaction tx = db.beginTx()) {
                node.addLabel(Routable);
                Relationship rel = poi.createRelationshipTo(node, ROUTE);
                rel.setProperty("distance", this.distance);
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
        public LocatedNode left;
        public LocatedNode right;
        public PointValue point;
        public LocatedNode poi;
        private CRSCalculator calculator;

        private LocationInterpolated(CRSCalculator calculator, LocatedNode poi, PointValue point, LocatedNode left, LocatedNode right) {
            this.left = left;
            this.right = right;
            this.point = point;
            this.poi = poi;
            this.calculator = calculator;
        }

        public Node process(GraphDatabaseService db) {
            Node node;
            try (Transaction tx = db.beginTx()) {
                left.node.addLabel(Routable);
                right.node.addLabel(Routable);
                node = db.createNode(Routable);
                node.setProperty("location", point);
                createConnection(node, left.node, calculator.distance(point, left.point));
                createConnection(node, right.node, calculator.distance(point, right.point));
                createConnection(poi.node, node, calculator.distance(point, poi.point));
                tx.success();
            }
            System.out.println("\t\tCreating interpolated node: " + node);
            return node;
        }

        private void createConnection(Node a, Node b, double distance) {
            Relationship rel = a.createRelationshipTo(b, ROUTE);
            rel.setProperty("distance", distance);
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
        public double leftDist;
        public double rightDist;

        private LocationIsPoint(Node node, Node left, double leftDist, Node right, double rightDist) {
            this.left = left;
            this.right = right;
            this.node = node;
            this.leftDist = leftDist;
            this.rightDist = rightDist;
        }

        public Node process(GraphDatabaseService db) {
            System.out.println("\t\tLinking point of interest node: " + node);
            try (Transaction tx = db.beginTx()) {
                left.addLabel(Routable);
                right.addLabel(Routable);
                node.addLabel(Routable);
                Relationship leftRel = node.createRelationshipTo(left, ROUTE);
                leftRel.setProperty("distance", leftDist);
                Relationship rightRel = node.createRelationshipTo(right, ROUTE);
                rightRel.setProperty("distance", rightDist);
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

    public static class IntersectionRoute {
        public Node fromNode;
        public Node wayNode;
        public Node toNode;
        public double distance;
        public Relationship fromRel;
        public Relationship toRel;

        public IntersectionRoute(Node node, Relationship wayNodeRel, Node wayNode) {
            this.fromNode = node;
            this.fromRel = wayNodeRel;
            this.wayNode = wayNode;
            this.toNode = null;
            this.toRel = null;
            this.distance = 0;
        }

        @Override
        public String toString() {
            return "IntersectionRoute:from(" + fromNode + ")via(" + wayNode + ")to(" + toNode + ")";
        }

        public boolean process(GraphDatabaseService db) {
            System.out.println("Searching for route from OSMNode:" + fromNode + " via OSMWayNode:" + wayNode);
            this.distance = 0;
            PathSegment pathSegment = findIntersection(db, wayNode);
            if (pathSegment == null || pathSegment.lastSegment().osmNode == null) {
                this.distance = 0;
                return false;
            } else {
                this.toNode = pathSegment.lastSegment().osmNode;
                this.toRel = pathSegment.lastRel();
                this.distance = pathSegment.totalDistance();
                return true;
            }
        }

        public ArrayList<Relationship> getExistingRoutes() {
            ArrayList<Relationship> existingRoutes = new ArrayList<>();
            for (Relationship rel : this.fromNode.getRelationships(OSMModel.ROUTE, Direction.BOTH)) {
                if (rel.getOtherNode(fromNode).equals(this.toNode)) {
                    existingRoutes.add(rel);
                }
            }
            return existingRoutes;
        }

        public Relationship mergeRouteRelationship() {
            ArrayList<Relationship> toDelete = new ArrayList<>();
            for (Relationship rel : this.fromNode.getRelationships(OSMModel.ROUTE, Direction.BOTH)) {
                if (rel.getOtherNode(fromNode).equals(this.toNode)) {
                    if (getRelIdFromProperty(rel, "fromRel") == fromRel.getId() && getRelIdFromProperty(rel, "toRel") == toRel.getId()) {
                        toDelete.add(rel);
                    }
                }
            }
            toDelete.forEach(Relationship::delete);
            Relationship rel = fromNode.createRelationshipTo(toNode, OSMModel.ROUTE);
            rel.setProperty("fromRel", fromRel.getId());
            rel.setProperty("toRel", toRel.getId());
            return rel;
        }

        private long getRelIdFromProperty(Relationship rel, String property) {
            if (rel.hasProperty(property)) {
                return (Long) rel.getProperty(property);
            } else {
                return -1;
            }
        }

        static class PathSegment {
            Node fromWayNode;
            Node toWayNode;
            Relationship lastRel;
            Node osmNode;
            double distance;
            int length;
            PathSegment nextSegment;

            PathSegment(Node fromWayNode) {
                this.fromWayNode = fromWayNode;
                this.toWayNode = null;
                this.osmNode = null;
                this.distance = 0;
                this.length = 0;
                this.nextSegment = null;
            }

            double totalDistance() {
                return distance + ((nextSegment != null) ? nextSegment.totalDistance() : 0);
            }

            double totalLength() {
                return length + ((nextSegment != null) ? nextSegment.totalLength() : 0);
            }

            Relationship lastRel() {
                return lastSegment().lastRel;
            }

            PathSegment lastSegment() {
                return (nextSegment == null) ? this : nextSegment.lastSegment();
            }

            boolean process(GraphDatabaseService db) {
                toWayNode = findLastWayNode(db, Direction.OUTGOING);
                if (toWayNode == null) {
                    toWayNode = findLastWayNode(db, Direction.INCOMING);
                }
                if (toWayNode != null) {
                    lastRel = toWayNode.getSingleRelationship(OSMModel.NODE, Direction.OUTGOING);
                    osmNode = lastRel.getEndNode();
                    return osmNode != null;
                } else {
                    return false;
                }
            }

            Node findLastWayNode(GraphDatabaseService db, Direction direction) {
                this.distance = 0;
                this.length = 0;
                Node lastNode = null;
                TraversalDescription traversalDescription = db.traversalDescription().depthFirst().relationships(OSMModel.NEXT, direction);
                for (Path path : traversalDescription.traverse(fromWayNode)) {
                    if (path.length() > 0) {
                        lastNode = path.endNode();
                        Relationship rel = path.lastRelationship();
                        if (rel.hasProperty("distance")) {
                            distance += (double) rel.getProperty("distance");
                            length++;
                        } else {
                            System.out.println("spatial.osm.routeIntersection(): Missing 'distance' on " + rel);
                            return null;
                        }
                    }
                }
                if (lastNode == null) {
                    System.out.println("spatial.osm.routeIntersection(): No " + direction + " path found from OSMWayNode(" + fromWayNode + ")");
                    return null;
                } else {
                    return lastNode;
                }
            }

            ArrayList<Relationship> nextWayRels() {
                ArrayList<Relationship> relationships = new ArrayList<>();
                if (osmNode != null) {
                    for (Relationship rel : osmNode.getRelationships(OSMModel.NODE, Direction.INCOMING)) {
                        if (!rel.equals(lastRel)) {
                            relationships.add(rel);
                        }
                    }
                }
                return relationships;
            }

            public String toString() {
                return "PathSegment[from:" + fromWayNode + ", to:" + toWayNode + ", length:" + length + ", distance:" + distance + "]" + (nextSegment == null ? "" : ".." + nextSegment);
            }
        }

        private PathSegment findIntersection(GraphDatabaseService db, Node startNode) {
            PathSegment pathSegment = new PathSegment(startNode);
            if (pathSegment.process(db)) {
                if (pathSegment.osmNode.hasLabel(OSMModel.Intersection)) {
                    System.out.println("\tFound labeled intersection: " + pathSegment.osmNode);
                    return pathSegment;
                } else {
                    ArrayList<Relationship> rels = pathSegment.nextWayRels();
                    if (rels.size() > 1) {
                        // Not a chain, but an intersection, let's stop here
                        System.out.println("\tFound unlabeled intersection: " + pathSegment.osmNode);
                        pathSegment.osmNode.addLabel(OSMModel.Intersection);
                        return pathSegment;
                    } else if (rels.size() == 1) {
                        // This is a connection in a chain, keep looking
                        System.out.println("\tFound chain link at " + pathSegment.osmNode + ", searching further...");
                        Node nextWayNode = rels.get(0).getStartNode();
                        pathSegment.nextSegment = findIntersection(db, nextWayNode);
                        return pathSegment;
                    } else {
                        // the end of a chain?
                        return null;
                    }
                }
            } else {
                return null;
            }
        }
    }
}
