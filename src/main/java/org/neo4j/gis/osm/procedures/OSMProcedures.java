package org.neo4j.gis.osm.procedures;

import org.neo4j.gis.osm.model.OSMModel;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class OSMProcedures {
    @Context
    public GraphDatabaseService db;

    @Procedure(value = "spatial.osm.routePointOfInterest", mode = Mode.WRITE)
    public Stream<PointRouteResult> findRouteToPointOfInterest(@Name("OSMNode") Node node, @Name("OSMWays") List<Node> ways) throws ProcedureException {
        try {
            OSMModel osm = new OSMModel(db);
            OSMModel.LocatedNode poi = osm.located(node);
            OSMModel.OSMWay closestWay = ways.stream().map(osm::way).min(osm.closestWay(poi)).orElseGet(() -> null);
            if (closestWay == null) {
                throw new ProcedureException(Status.Procedure.ProcedureCallFailed, "Failed to find closest way from list of %d ways to node %s", ways.size(), node);
            }
            System.out.println("spatial.osm.routePointOfInterest(" + node + ") located closest way: " + closestWay);
            OSMModel.LocationMaker locationMaker = closestWay.getClosest(poi).getLocationMaker();
            Node connected = locationMaker.process(db);
            System.out.println("spatial.osm.routePointOfInterest(" + node + ") created connected node: " + connected);
            return Stream.of(new PointRouteResult(connected));
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Procedure(value = "spatial.osm.routeIntersection", mode = Mode.WRITE)
    public Stream<IntersectionRouteResult> findStreetRoute(@Name("OSMNode") Node node, @Name("deleteExistingRoutes") boolean deleteExistingRoutes, @Name("createNewRoutes") boolean createNewRoutes) throws ProcedureException {
        try {
            OSMModel osm = new OSMModel(db);
            ArrayList<OSMModel.IntersectionRoute> routesToSearch = new ArrayList<>();
            for (Relationship rel : node.getRelationships(OSMModel.NODE, Direction.INCOMING)) {
                routesToSearch.add(osm.intersectionRoute(node, rel, rel.getStartNode()));
            }
            ArrayList<IntersectionRouteResult> routesFound = new ArrayList<>();
            Collections.reverse(routesToSearch);
            for (OSMModel.IntersectionRoute route : routesToSearch) {
                if (route.process(db)) {
                    if (!deleteExistingRoutes && route.getExistingRoutes().size() > 0) {
                        System.out.println("Already have existing routes between " + route.fromNode + " and " + route.toNode);
                    } else {
                        if (deleteExistingRoutes) {
                            route.getExistingRoutes().forEach(Relationship::delete);
                        }
                        if (createNewRoutes) {
                            route.mergeRouteRelationship();
                        }
                        routesFound.add(new IntersectionRouteResult(route));
                    }
                } else {
                    System.out.println("Failed to find a route for " + route);
                }
            }
            System.out.println("Found " + routesFound.size() + " routes from " + node);
            return routesFound.stream();
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static class IntersectionRouteResult {
        public Node fromNode;
        public Node wayNode;
        public Node toNode;
        public double distance;
        public Relationship fromRel;
        public Relationship toRel;

        public IntersectionRouteResult(OSMModel.IntersectionRoute route) {
            this.fromNode = route.fromNode;
            this.fromRel = route.fromRel;
            this.wayNode = route.wayNode;
            this.toNode = route.toNode;
            this.toRel = route.toRel;
            this.distance = route.distance;
        }
    }

    public class PointRouteResult {
        public Node node;

        public PointRouteResult(Node routeNode) {
            this.node = routeNode;
        }
    }
}
