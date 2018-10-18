package org.neo4j.gis.osm.model;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.values.storable.CRSCalculator;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.util.*;

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

    public IntersectionRoutes intersectionRoutes(Node osmNode, Relationship relToStartWayNode, Node startOsmWayNode, boolean addLabels) {
        return new IntersectionRoutes(osmNode, relToStartWayNode, startOsmWayNode, addLabels);
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
        public ArrayList<Node> wayNodes;
        public ArrayList<LocatedNode> nodes;
        HashMap<Long, LocatedNode> seenNodes;
        OSMWay(Node wayNode) {
            if (!wayNode.hasLabel(OSMWay))
                throw new IllegalArgumentException("Way node does not have :OSMWay label: " + wayNode);
            Relationship tagsRel = wayNode.getSingleRelationship(TAGS, Direction.OUTGOING);
            if (tagsRel == null)
                throw new IllegalArgumentException("Way node does not have outgoing :TAGS relationship: " + wayNode);
            this.wayNode = wayNode;
            Node tagsNode = tagsRel.getEndNode();
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

        public OSMWayDistance closeTo(LocatedNode poi){
            return new OSMWayDistance(this, poi);
        }

        public String getName() {
            return (name == null) ? "<unknown>" : name;
        }

        @Override
        public String toString() {
            return "OSMWay[" + wayNode.getId() + "]: name:" + name + ",  length:" + nodes.size();
        }
    }

    public class OSMWayDistance {
        OSMWay way;
        LocatedNode node;
        DistanceResult closest;

        private OSMWayDistance(OSMWay way, LocatedNode node) {
            this.way = way;
            this.node = node;
            calculateClosestDistance();
        }

        private void calculateClosestDistance() {
            closest = new DistanceResult(node);
            for (int i = 0; i < way.nodes.size(); i++) {
                LocatedNode n = way.nodes.get(i);
                if (!n.point.getCoordinateReferenceSystem().equals(closest.crs)) {
                    throw new IllegalArgumentException("Cannot compare points of different crs: " + n.point.getCoordinateReferenceSystem() + " != " + closest.crs);
                }
                double distance = closest.calculator.distance(node.point, n.point);
                if (distance < closest.nodeDistance) {
                    closest.nodeDistance = distance;
                    closest.closestNodeIndex = i;
                }
            }
            closest.locationMaker = closest.makeLocationMaker();
        }

        public LocationMaker getLocationMaker() {
            return closest.getLocationMaker();
        }

        public class DistanceResult {
            LocatedNode node;
            double nodeDistance;
            int closestNodeIndex;
            CoordinateReferenceSystem crs;
            public CRSCalculator calculator;
            private LocationMaker locationMaker;

            DistanceResult(LocatedNode node) {
                this.node = node;
                crs = node.point.getCoordinateReferenceSystem();
                calculator = crs.getCalculator();
                nodeDistance = Double.MAX_VALUE;
                closestNodeIndex = -1;
            }

            @Override
            public String toString() {
                Object closest = (closestNodeIndex < 0 || closestNodeIndex >= way.nodes.size()) ? "null" : way.nodes.get(closestNodeIndex);
                return "DistanceResult: distance:" + nodeDistance + " from " + node + " to " + closest;
            }

            LocationMaker getLocationMaker() {
                return this.locationMaker;
            }

            private LocationMaker makeLocationMaker() {
                if (closestNodeIndex < 0) {
                    throw new IllegalStateException("No closest node known - has closestDistanceTo(node) not been called?");
                }
                if (closestNodeIndex == 0) {
                    if (way.nodes.size() > 1) {
                        return makeLocationMaker(0, 1);
                    } else {
                        return new LocationExists(node.node, way.nodes.get(0).node, nodeDistance);
                    }
                }
                if (closestNodeIndex == way.nodes.size() - 1) {
                    return makeLocationMaker(closestNodeIndex - 1, closestNodeIndex);
                } else {
                    double left = calculator.distance(way.nodes.get(closestNodeIndex - 1).point, node.point);
                    double right = calculator.distance(way.nodes.get(closestNodeIndex + 1).point, node.point);
                    if (left < right) {
                        return makeLocationMaker(closestNodeIndex - 1, closestNodeIndex);
                    } else {
                        return makeLocationMaker(closestNodeIndex, closestNodeIndex + 1);
                    }
                }
            }

            private LocationMaker makeLocationMaker(int leftIndex, int rightIndex) {
                LocatedNode left = way.nodes.get(leftIndex);
                LocatedNode right = way.nodes.get(rightIndex);
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
        double getDistance();
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

        @Override
        public double getDistance() {
            return distance;
        }

        @Override
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
     * This class represents the case where the point of interest lies between two nodes, but not on the line
     * between them so we create a new node on the street (between the two nodes) and link the original node
     * to that, and that to the street nodes in the routable graph.
     */
    public static class LocationInterpolated implements LocationMaker {
        public LocatedNode left;
        public LocatedNode right;
        public PointValue point;
        public LocatedNode poi;
        private CRSCalculator calculator;
        private double distance;

        private LocationInterpolated(CRSCalculator calculator, LocatedNode poi, PointValue point, LocatedNode left, LocatedNode right) {
            this.left = left;
            this.right = right;
            this.point = point;
            this.poi = poi;
            this.calculator = calculator;
            this.distance = calculator.distance(point, poi.point);
        }

        @Override
        public double getDistance() {
            return distance;
        }

        @Override
        public Node process(GraphDatabaseService db) {
            Node node;
            try (Transaction tx = db.beginTx()) {
                left.node.addLabel(Routable);
                right.node.addLabel(Routable);
                node = db.createNode(Routable);
                node.setProperty("location", point);
                createConnection(node, left.node, calculator.distance(point, left.point));
                createConnection(node, right.node, calculator.distance(point, right.point));
                createConnection(poi.node, node, this.distance);
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

        @Override
        public double getDistance() {
            return 0.0;
        }

        @Override
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

    public static class ClosestWay implements Comparator<OSMWayDistance> {
        public int compare(OSMWayDistance o1, OSMWayDistance o2) {
            return Double.compare(o1.closest.locationMaker.getDistance(), o2.closest.locationMaker.getDistance());
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

    /**
     * A complete route from a starting node to an ending intersection node. Is created based from a chain of
     * PathSegments that are used to build up the route.
     */
    public static class IntersectionRoute {
        public Node fromNode;
        public Node wayNode;
        public Node toNode;
        public double distance;
        public long length;
        public long count;
        public Relationship fromRel;
        public Relationship toRel;

        public IntersectionRoute(Node node, Relationship wayNodeRel, Node wayNode, IntersectionRoutes.PathSegment pathSegment) {
            this.fromNode = node;
            this.fromRel = wayNodeRel;
            this.wayNode = wayNode;
            this.toNode = pathSegment.lastSegment().osmNode;
            this.toRel = pathSegment.lastRel();
            this.distance = pathSegment.totalDistance();
            this.length = pathSegment.totalLength();
            this.count = pathSegment.countSegments();
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

        @Override
        public String toString() {
            return "IntersectionRoute:from(" + fromNode + ")via(" + wayNode + ")to(" + toNode + ")distance(" + distance + ")";
        }

    }

    public static class IntersectionRoutes {
        private Node fromNode;
        private Node wayNode;
        private Relationship fromRel;
        private boolean addLabels;
        private int maxDepth;
        private HashSet<Node> previouslySeen;
        public List<IntersectionRoute> routes;

        public IntersectionRoutes(Node node, Relationship wayNodeRel, Node wayNode, boolean addLabels) {
            this.fromNode = node;
            this.fromRel = wayNodeRel;
            this.wayNode = wayNode;
            this.addLabels = addLabels;
            this.maxDepth = 20;
            this.previouslySeen = new HashSet<>();
            this.routes = new ArrayList<>();
        }

        @Override
        public String toString() {
            return "IntersectionRoutes:from(" + fromNode + ")via(" + wayNode + ")";
        }

        public boolean process(GraphDatabaseService db) {
            System.out.println("Searching for route from OSMNode:" + fromNode + " via OSMWayNode:" + wayNode);
            for (PathSegmentTree tree : findIntersections(db, wayNode, 0)) {
                for (PathSegment path : tree.asPathSegments()) {
                    routes.add(new IntersectionRoute(fromNode, fromRel, wayNode, path));
                }
            }
            return routes.size() > 0;
        }


        static class PathSegment {
            Node fromWayNode;
            Node toWayNode;
            Relationship lastRel;
            Node osmNode;
            double distance;
            int length;
            PathSegment nextSegment;

            PathSegment(PathSegmentTree parent, PathSegment child) {
                this.fromWayNode = parent.fromWayNode;
                this.toWayNode = parent.toWayNode;
                this.lastRel = parent.lastRel;
                this.osmNode = parent.osmNode;
                this.distance = parent.distance;
                this.length = parent.length;
                this.nextSegment = child;
            }

            double totalDistance() {
                return distance + ((nextSegment != null) ? nextSegment.totalDistance() : 0);
            }

            int totalLength() {
                return length + ((nextSegment != null) ? nextSegment.totalLength() : 0);
            }

            Relationship lastRel() {
                return lastSegment().lastRel;
            }

            PathSegment lastSegment() {
                return (nextSegment == null) ? this : nextSegment.lastSegment();
            }

            int countSegments() {
                return 1 + ((nextSegment == null) ? 0 : nextSegment.countSegments());
            }

            public String toString() {
                return "PathSegment[from:" + fromWayNode + ", to:" + toWayNode + ", length:" + length + ", distance:" + distance + "]" + (nextSegment == null ? "" : ".." + nextSegment);
            }
        }

        static class PathSegmentTree {
            Node fromWayNode;
            Direction direction;
            Node toWayNode;
            Relationship lastRel;
            Node osmNode;
            double distance;
            int length;
            List<PathSegmentTree> childSegments;

            PathSegmentTree(Node fromWayNode, Direction direction) {
                this.fromWayNode = fromWayNode;
                this.direction = direction;
                this.toWayNode = null;
                this.osmNode = null;
                this.distance = 0;
                this.length = 0;
                this.childSegments = null;
            }

            List<PathSegment> asPathSegments() {
                ArrayList<PathSegment> pathSegments = new ArrayList<>();
                if (childSegments != null && childSegments.size() > 0) {
                    for (PathSegmentTree child : childSegments) {
                        for (PathSegment childSegment : child.asPathSegments()) {
                            pathSegments.add(new PathSegment(this, childSegment));
                        }
                    }
                } else {
                    pathSegments.add(new PathSegment(this, null));
                }
                return pathSegments;
            }

            boolean process(GraphDatabaseService db) {
                traverseToFirstIntersection(db);
                return osmNode != null;
            }

            private void traverseToFirstIntersection(GraphDatabaseService db) {
                this.distance = 0;
                this.length = 0;
                TraversalDescription traversalDescription = db.traversalDescription().depthFirst().relationships(OSMModel.NEXT, direction);
                for (Path path : traversalDescription.traverse(fromWayNode)) {
                    if (path.length() > 0) {
                        toWayNode = path.endNode();
                        Relationship rel = path.lastRelationship();
                        if (rel.hasProperty("distance")) {
                            distance += (double) rel.getProperty("distance");
                            length++;
                            lastRel = toWayNode.getSingleRelationship(OSMModel.NODE, Direction.OUTGOING);
                            osmNode = lastRel.getEndNode();
                            if (osmNode.hasLabel(OSMModel.Intersection)) {
                                // stop searching
                                break;
                            }
                        } else {
                            System.out.println("spatial.osm.routeIntersection(): Missing 'distance' on " + rel);
                            osmNode = null;
                            break;
                        }
                    }
                }
                if (toWayNode == null) {
                    // TODO: Probably we started at the last node for this direction, so need not print anything here
                    System.out.println("spatial.osm.routeIntersection(): No " + direction + " path found from OSMWayNode(" + fromWayNode + ")");
                } else if (osmNode == null) {
                    System.out.println("spatial.osm.routeIntersection(): No intersection node found in " + direction + " path found from OSMWayNode(" + fromWayNode + ")");
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
                return "PathSegmentTree[from:" + fromWayNode + ", to:" + toWayNode + ", length:" + length + ", distance:" + distance + "]" + (childSegments == null ? "" : ".. and " + childSegments.size() + " child branches");
            }
        }

        private List<PathSegmentTree> findIntersections(GraphDatabaseService db, Node startNode, int depth) {
            List<PathSegmentTree> pathSegmentTrees = new ArrayList<>();
            for (Direction direction : new Direction[]{Direction.OUTGOING, Direction.INCOMING}) {
                PathSegmentTree pathSegmentTree = findIntersection(db, startNode, direction, depth);
                if (pathSegmentTree != null && pathSegmentTree.osmNode != null) {
                    pathSegmentTrees.add(pathSegmentTree);
                }
            }
            return pathSegmentTrees;
        }

        private PathSegmentTree findIntersection(GraphDatabaseService db, Node startNode, Direction direction, int depth) {
            PathSegmentTree pathSegment = new PathSegmentTree(startNode, direction);
            if (depth < maxDepth && pathSegment.process(db)) {
                if (previouslySeen.contains(pathSegment.osmNode)) {
                    System.out.println("\tAlready processed potential intersection node, rejecting cyclic route: " + pathSegment.osmNode);
                    return null;
                }
                previouslySeen.add(pathSegment.osmNode);
                if (pathSegment.osmNode.hasLabel(OSMModel.Intersection)) {
                    System.out.println("\tFound labeled intersection: " + pathSegment.osmNode);
                    return pathSegment;
                } else {
                    ArrayList<Relationship> rels = pathSegment.nextWayRels();
                    if (rels.size() > 1) {
                        // Not a chain, but an intersection, let's stop here
                        if (addLabels) {
                            System.out.println("\tFound unlabeled intersection (will add label and include): " + pathSegment.osmNode);
                            pathSegment.osmNode.addLabel(OSMModel.Intersection);
                            return pathSegment;
                        } else {
                            System.out.println("\tFound unlabeled intersection (will not add label, rejecting): " + pathSegment.osmNode);
                            return null;
                        }
                    } else if (rels.size() == 1) {
                        // This is a connection in a chain, keep looking in the same direction
                        // TODO: Look in two directions (branching the chain, so needs a different storage than nextSegement)
                        System.out.println("\tFound chain link at " + pathSegment.osmNode + ", searching further...");
                        Node nextWayNode = rels.get(0).getStartNode();
                        pathSegment.childSegments = findIntersections(db, nextWayNode, depth + 1);
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
