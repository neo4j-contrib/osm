package org.neo4j.gis.osm.procedures;

import org.neo4j.gis.osm.OSMModel;
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

    @Procedure(value = "spatial.osm.route", mode = Mode.WRITE)
    public Stream<PointRouteResult> findStreetRoute(@Name("OSMNode") Node node, @Name("OSMWays") List<Node> ways) throws ProcedureException {
        OSMModel osm = new OSMModel(db);
        OSMModel.LocatedNode poi = osm.located(node);
        OSMModel.OSMWay closestWay = ways.stream().map(osm::way).min(osm.closestWay(poi)).orElseGet(() -> null);
        if (closestWay == null) {
            throw new ProcedureException(Status.Procedure.ProcedureCallFailed, "Failed to find closest way from list of %d ways to node %s", ways.size(), node);
        }
        OSMModel.LocationMaker locationMaker = closestWay.getClosest().getLocationMaker();
        Node connected = locationMaker.process(db);
        return Stream.of(new PointRouteResult(connected));
    }

    public class PointRouteResult {
        public Node node;

        public PointRouteResult(Node routeNode) {
            this.node = routeNode;
        }
    }
}
