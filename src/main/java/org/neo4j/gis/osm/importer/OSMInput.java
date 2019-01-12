package org.neo4j.gis.osm.importer;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.*;
import org.neo4j.values.storable.*;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.ToIntFunction;

import static org.neo4j.gis.spatial.SpatialConstants.*;

public class OSMInput implements Input {
    private final String[] osmFiles;
    private final Groups groups = new Groups();
    private final Group nodesGroup;
    private final Group waysGroup;
    private final Group wayNodesGroup;
    private final Group relationsGroup;
    private final Group tagsGroup;
    private final Group miscGroup;
    private final Collector badCollector;
    private final RangeFilter range;
    private final FileSystemAbstraction fs;

    // This uses an internal API of Neo4j, which could be a compatibility issue moving forward
    // Two options: get Neo4j to make this public, or we create our own version here. The
    // CRS is easy, but the calculator is less so.
    private final CoordinateReferenceSystem wgs84 = CoordinateReferenceSystem.WGS84;
    private final CRSCalculator calculator = wgs84.getCalculator();
    private final Configuration config;

    public OSMInput(FileSystemAbstraction fs, String[] osmFiles, Configuration config, Collector badCollector, RangeFilter range) {
        this.fs = fs;
        this.osmFiles = osmFiles;
        this.config = config;
        this.badCollector = badCollector;
        this.range = range;
        nodesGroup = this.groups.getOrCreate("osm_nodes");
        waysGroup = this.groups.getOrCreate("osm_ways");
        wayNodesGroup = this.groups.getOrCreate("osm_way_nodes");
        relationsGroup = this.groups.getOrCreate("osm_relations");
        tagsGroup = this.groups.getOrCreate("osm_tags");
        miscGroup = this.groups.getOrCreate("osm_misc");
    }

    public enum RoadDirection {
        BOTH, FORWARD, BACKWARD;
    }

    /**
     * Retrieves the direction of the given road, i.e. whether it is a one-way road from its start node,
     * a one-way road to its start node or a two-way road.
     *
     * <ul>
     * <li><code>"oneway" = "-1"</code> BACKWARD</li>
     * <li><code>"oneway" = "1" || "yes" || "true"</code> FORWARD</li>
     * <li>Anything else means BOTH (not one-way)</li>
     * </ul>
     *
     * @param wayProperties the property map of the road
     * @return BOTH if it's a two-way road, FORWARD if it's a one-way road from the start node,
     * or BACKWARD if it's a one-way road to the start node
     */
    public static RoadDirection getRoadDirection(Map<String, Object> wayProperties) {
        String oneway = (String) wayProperties.get("oneway");
        if (null != oneway) {
            if ("-1".equals(oneway))
                return RoadDirection.BACKWARD;
            if ("1".equals(oneway) || "yes".equalsIgnoreCase(oneway) || "true".equalsIgnoreCase(oneway))
                return RoadDirection.FORWARD;
        }
        return RoadDirection.BOTH;
    }

    private static class NodeEvent {
        String label;
        String osmId;
        Group group;
        Map<String, Object> properties;
        private static final Map<String, Object> EMPTY_PROPERTIES = new HashMap<>();

        private NodeEvent(String label, String osmId, Group group, Map<String, Object> properties) {
            this.label = label;
            this.osmId = osmId;
            this.group = group;
            this.properties = properties;
        }

        private NodeEvent(String label, String osmId, Group group) {
            this(label, osmId, group, EMPTY_PROPERTIES);
        }

        public boolean equals(Object obj) {
            if (obj instanceof NodeEvent) {
                NodeEvent other = (NodeEvent) obj;
                return other.group == this.group && other.osmId.equals(this.osmId);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return this.getClass().getName() + ":" + label + "[" + osmId + "]";
        }
    }

    private class OSMNode extends NodeEvent {
        private OSMNode(long id, Map<String, Object> properties) {
            super("OSMNode", "n" + id, nodesGroup, properties);
        }

        private OSMNode(long id) {
            super("OSMNode", "n" + id, nodesGroup);
        }
    }

    private class OSMWayNode extends NodeEvent {
        private OSMWayNode(long wayId, long nodeId) {
            // TODO: Save the wayId to the node to facilitate repair of partial way imports later (eg. first node not connected)
            //super("OSMWayNode", "w" + wayId + "n" + nodeId, wayNodesGroup, Collections.singletonMap("way_osm_id", wayId));
            super("OSMWayNode", "w" + wayId + "n" + nodeId, wayNodesGroup);
        }
    }

    private class OSMWay extends NodeEvent {
        private OSMWay(long id, Map<String, Object> properties) {
            super("OSMWay", "w" + id, waysGroup, properties);
        }
    }

    private class OSMRelation extends NodeEvent {
        private OSMRelation(long id, Map<String, Object> properties) {
            super("OSMRelation", "r" + id, relationsGroup, properties);
        }
    }

    private class OSMTags extends NodeEvent {
        private OSMTags(String id, Map<String, Object> properties) {
            super("OSMTags", "t" + id, tagsGroup, properties);
        }
    }

    private class OSMMisc extends NodeEvent {
        private OSMMisc(String label, String id, Map<String, Object> properties) {
            super(label, id, miscGroup, properties);
        }
    }

    interface OSMInputChunk extends InputChunk {

        void addDatasetNode(String name, Map<String, Object> properties);

        void addDatasetBoundsNode(String name, Map<String, Object> properties);

        void addOSMNode(long id, Map<String, Object> properties);

        void addOSMWay(long id, Map<String, Object> properties, List<Long> wayNodes, Map<String, Object> wayTags);

        void addOSMRelation(long id, Map<String, Object> properties, ArrayList<Map<String, Object>> relationMembers, Map<String, Object> relationTags);

        void addOSMTags(Map<String, Object> properties);

        boolean insideTaggableEvent();

        void endTaggableEvent();

        long size();

        void reset();
    }

    class OSMInputChunkFunctions {
        OSMMisc dataset(String name, Map<String, Object> properties) {
            if (properties != null && !properties.containsKey("name")) properties.put("name", name);
            return new OSMMisc("OSM", "osm_" + name, properties);
        }

        OSMMisc bounds(String name, Map<String, Object> properties) {
            return new OSMMisc("Bounds", "bounds_" + name, properties);
        }

        OSMWay way(long id, Map<String, Object> properties, Map<String, Object> wayTags, RoadDirection direction) {
            String name = (String) wayTags.get("name");
            boolean isRoad = wayTags.containsKey("highway");
            if (isRoad) {
                properties.put("oneway", direction.toString());
                properties.put("highway", wayTags.get("highway"));
            }
            if (name != null) {
                // Copy name tag to way because this seems like a valuable location for such a property
                properties.put("name", name);
            }
            return new OSMWay(id, properties);
        }
    }

    class OSMNodesInputChunk extends OSMInputChunkFunctions implements OSMInputChunk {
        ArrayList<NodeEvent> data = new ArrayList<>(config.batchSize());
        NodeEvent previousTaggableNodeEvent = null;
        int currentRead = -1;

        private void addEvent(NodeEvent event) {
            data.add(event);
        }

        @Override
        public void addDatasetNode(String name, Map<String, Object> properties) {
            addEvent(dataset(name, properties));
        }

        @Override
        public void addDatasetBoundsNode(String name, Map<String, Object> properties) {
            addEvent(bounds(name, properties));
        }

        @Override
        public void addOSMNode(long id, Map<String, Object> properties) {
            previousTaggableNodeEvent = new OSMNode(id, properties);
            addEvent(previousTaggableNodeEvent);
        }

        @Override
        public void addOSMWay(long id, Map<String, Object> properties, List<Long> wayNodes, Map<String, Object> wayTags) {
            RoadDirection direction = getRoadDirection(wayTags);
            previousTaggableNodeEvent = way(id, properties, wayTags, direction);
            addEvent(previousTaggableNodeEvent);
            HashSet<Long> madeWayNodes = new HashSet<>(wayNodes.size());   // TODO: Find a less GC sensitive way
            for (long osmId : wayNodes) {
                if (!madeWayNodes.contains(osmId)) {
                    madeWayNodes.add(osmId);
                    addEvent(new OSMWayNode(id, osmId));
                }
            }
        }

        @Override
        public void addOSMRelation(long id, Map<String, Object> properties, ArrayList<Map<String, Object>> relationMembers, Map<String, Object> relationTags) {
            previousTaggableNodeEvent = new OSMRelation(id, properties);
            addEvent(previousTaggableNodeEvent);
            // Currently no additional nodes are made because only relationships are made betwen the OSMRelation and the referenced nodes.
            // However, if we figure out a way to create the geometry node during import, we could add that here too
        }

        @Override
        public void addOSMTags(Map<String, Object> properties) {
            if (insideTaggableEvent()) {
                augmentProperties("name", properties, previousTaggableNodeEvent.properties);
                addEvent(new OSMTags(previousTaggableNodeEvent.osmId, properties));
            } else {
                error("Unexpected null parent node for tags: " + properties);
            }
        }

        private void augmentProperties(String key, Map<String, Object> from, Map<String, Object> to) {
            if (from.containsKey(key) && !to.containsKey(key)) {
                to.put(key, from.get(key));
            }
        }

        @Override
        public boolean insideTaggableEvent() {
            return this.previousTaggableNodeEvent != null;
        }

        @Override
        public void endTaggableEvent() {
            this.previousTaggableNodeEvent = null;
        }

        @Override
        public long size() {
            return data.size();
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            currentRead++;
            if (data != null && currentRead < data.size()) {
                // Make OSM node
                NodeEvent event = data.get(currentRead);
                visitor.id(event.osmId, event.group);
                visitor.labels(new String[]{event.label});
                event.properties.forEach(visitor::property);
                visitor.endOfEntity();
                return true;
            }
            return false;
        }

        @Override
        public void reset() {
            this.data.clear();
            this.currentRead = -1;
        }

        @Override
        public void close() {
            reset();
        }
    }

    private static class RelationshipEvent {
        String type;
        String fromId;
        String toId;
        Group fromGroup;
        Group toGroup;
        Map<String, Object> properties = EMPTY_PROPERTIES;
        private static final Map<String, Object> EMPTY_PROPERTIES = new HashMap<>();

        private RelationshipEvent(String type, NodeEvent from, NodeEvent to) {
            this.type = type;
            this.fromId = from.osmId;
            this.toId = to.osmId;
            this.fromGroup = from.group;
            this.toGroup = to.group;
        }

        private RelationshipEvent(String type, NodeEvent from, NodeEvent to, Map<String, Object> properties) {
            this(type, from, to);
            this.properties = properties;
        }
    }

    private class OSMTagsRel extends RelationshipEvent {
        private OSMTagsRel(NodeEvent from, OSMTags to) {
            super("TAGS", from, to);
        }
    }

    private class OSMWayNodeRel extends RelationshipEvent {
        private OSMWayNodeRel(OSMWayNode from, OSMNode to) {
            super("NODE", from, to);
        }
    }

    private class OSMNextWayNodeRel extends RelationshipEvent {
        private OSMNextWayNodeRel(OSMWayNode from, OSMWayNode to) {
            super("NEXT", from, to);
        }
    }

    private class OSMFirstWayNodeRel extends RelationshipEvent {
        private OSMFirstWayNodeRel(OSMWay from, OSMWayNode to) {
            super("FIRST_NODE", from, to);
        }
    }

    private class OSMBoundsRel extends RelationshipEvent {
        private OSMBoundsRel(OSMMisc from, OSMMisc to) {
            super("BBOX", from, to);
        }
    }

    private class OSMRelationMemberRel extends RelationshipEvent {
        private OSMRelationMemberRel(OSMRelation from, NodeEvent to, Map<String, Object> properties) {
            super("MEMBER", from, to, properties);
        }
    }

    class OSMRelationshipsInputChunk extends OSMInputChunkFunctions implements OSMInputChunk {
        ArrayList<RelationshipEvent> data = new ArrayList<>(config.batchSize());
        NodeEvent previousTaggableNodeEvent = null;
        int currentRead = -1;

        private void addEvent(RelationshipEvent event) {
            data.add(event);
        }

        @Override
        public void addDatasetNode(String name, Map<String, Object> properties) {
        }

        @Override
        public void addDatasetBoundsNode(String name, Map<String, Object> properties) {
            addEvent(new OSMBoundsRel(dataset(name, null), bounds(name, properties)));
        }

        @Override
        public void addOSMNode(long id, Map<String, Object> properties) {
            previousTaggableNodeEvent = new OSMNode(id, properties);
        }

        @Override
        public void addOSMWay(long wayId, Map<String, Object> properties, List<Long> wayNodes, Map<String, Object> wayTags) {
            RoadDirection direction = getRoadDirection(wayTags);
            OSMWay osmWay = way(wayId, properties, wayTags, direction);
            int geometry = GTYPE_LINESTRING;
            OSMWayNode previousWayNode = null;
            OSMNode firstNode = null;
            OSMNode previousNode = null;
            LinkedHashMap<String, Object> relProps = new LinkedHashMap<String, Object>();
            HashSet<Long> madeWayNodes = new HashSet<>(wayNodes.size());   // TODO: Find a less GC sensitive way
            for (long osmId : wayNodes) {
                OSMNode osmNode = new OSMNode(osmId);
                if (osmNode.equals(previousNode)) {
                    continue;
                }
                OSMWayNode wayNode = new OSMWayNode(wayId, osmId);
                // link each proxy node to the actual point node, unless we have loops
                if (!madeWayNodes.contains(osmId)) {
                    madeWayNodes.add(osmId);
                    addEvent(new OSMWayNodeRel(wayNode, osmNode));
                }
                // Link the way to the first proxy node
                if (firstNode == null) {
                    addEvent(new OSMFirstWayNodeRel(osmWay, wayNode));
                }
                if (previousWayNode != null) {
                    // link each proxy node to the next proxy node.
                    // We default to bi-directional (and don't store direction in the way node), but if it
                    // is one-way we mark it as such, and define the direction using the relationship direction
                    if (direction == RoadDirection.BACKWARD) {
                        addEvent(new OSMNextWayNodeRel(wayNode, previousWayNode));
                    } else {
                        addEvent(new OSMNextWayNodeRel(previousWayNode, wayNode));
                    }
                }
                previousWayNode = wayNode;
                previousNode = osmNode;
                if (firstNode == null) {
                    firstNode = osmNode;
                }
            }
            if (firstNode != null && firstNode.equals(previousNode)) {
                geometry = GTYPE_POLYGON;
            }
            if (wayNodes.size() < 2) {
                geometry = GTYPE_POINT;
            }
            previousTaggableNodeEvent = osmWay;
            //TODO: Add geometry
            //addNodeGeometry( way, geometry, bbox, wayNodes.size() );
        }

        @Override
        public void addOSMRelation(long id, Map<String, Object> properties, ArrayList<Map<String, Object>> relationMembers, Map<String, Object> relationTags) {
            OSMRelation osmRelation = new OSMRelation(id, properties);
            NodeEvent prevMember = null;
            Map<String, Object> relProps = new HashMap<>(1);
            for (Map<String, Object> memberProps : relationMembers) {
                String memberType = (String) memberProps.get("type");
                long member_ref = Long.parseLong(memberProps.get("ref").toString());
                if (memberType != null) {
                    NodeEvent member;
                    switch (memberType) {
                        case "node":
                            member = new OSMNode(member_ref, null);
                            break;
                        case "way":
                            member = new OSMWay(member_ref, null);
                            break;
                        case "relation":
                            member = new OSMRelation(member_ref, null);
                            break;
                        default:
                            error("Unknown member type: " + memberProps.toString());
                            continue;
                    }
                    if (member.equals(prevMember)) {
                        continue;
                    }
                    if (member.equals(osmRelation)) {
                        error("Cannot add relation to same member: relation[" + relationTags + "] - member[" + memberProps + "]");
                        continue;
                    }
                    //TODO: Create and manage GeometryMetaData (bounding box and geometry type)
                    relProps.clear();
                    String role = (String) memberProps.get("role");
                    if (role != null && role.length() > 0) {
                        relProps.put("role", role);
                        if (role.equals("outer")) {
                            // TODO: Set metaGeom.setPolygon();
                        }
                    }
                    addEvent(new OSMRelationMemberRel(osmRelation, member, relProps));
                    prevMember = member;
                } else {
                    error("Cannot process invalid relation member: " + memberProps.toString());
                }
            }
            previousTaggableNodeEvent = osmRelation;
        }

        @Override
        public void addOSMTags(Map<String, Object> properties) {
            if (insideTaggableEvent()) {
                OSMTags tagNode = new OSMTags(previousTaggableNodeEvent.osmId, properties);
                OSMTagsRel tagsRel = new OSMTagsRel(previousTaggableNodeEvent, tagNode);
                //System.out.println("Creating relationship: (" + tagsRel.fromId + ")-[" + tagsRel.type + "]->(" + tagsRel.toId + ")");
                addEvent(tagsRel);
            } else {
                error("Unexpectedly null parent for tags: " + properties);
            }
        }

        @Override
        public boolean insideTaggableEvent() {
            return this.previousTaggableNodeEvent != null;
        }

        @Override
        public void endTaggableEvent() {
            this.previousTaggableNodeEvent = null;
        }

        @Override
        public long size() {
            return data.size();
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            currentRead++;
            if (data != null && currentRead < data.size()) {
                // Make relationship between two nodes, with nodeId mapped using groups
                RelationshipEvent event = data.get(currentRead);
                visitor.startId(event.fromId, event.fromGroup);
                visitor.endId(event.toId, event.toGroup);
                visitor.type(event.type);
                event.properties.forEach(visitor::property);
                visitor.endOfEntity();
                return true;
            }
            return false;
        }

        @Override
        public void reset() {
            this.data.clear();
            this.currentRead = -1;
        }

        @Override
        public void close() {
            reset();
        }
    }

    // "2008-06-11T12:36:28Z"
    private DateTimeFormatter timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private abstract class OSMInputIterator implements InputIterator {
        private final String osmFile;
        private final XMLStreamReader parser;
        private final ArrayList<Long> wayNodes = new ArrayList<>();
        private final ArrayList<Map<String, Object>> relationMembers = new ArrayList<>();
        private Map<String, Object> wayProperties = null;
        private Map<String, Object> relationProperties = null;
        private int depth = 0;
        private ArrayList<String> currentXMLTags = new ArrayList<>();
        private Map<String, Object> currentNodeTags = new LinkedHashMap<>();

        private OSMInputIterator(String osmFile) {
            this.osmFile = osmFile;
            this.parser = getXMLParser();
        }

        @Override
        public synchronized boolean next(InputChunk chunk) {
            OSMInputChunk events = (OSMInputChunk) chunk;
            events.reset();
            while (events.size() < config.batchSize() || events.insideTaggableEvent()) {
                try {
                    if (parser.hasNext()) {
                        int event = parser.next();
                        if (event == javax.xml.stream.XMLStreamConstants.END_DOCUMENT) {
                            break;
                        }
                        switch (event) {
                            case javax.xml.stream.XMLStreamConstants.START_ELEMENT:
                                currentXMLTags.add(depth, parser.getLocalName());
                                if (currentXMLTags.get(depth).equals("tag")) {
                                    // add 'tag' to currentRead tag collection (to be saved at end of parent node)
                                    Map<String, Object> properties = extractProperties(parser);
                                    currentNodeTags.put(properties.get("k").toString(), properties.get("v").toString());
                                } else if (currentXMLTags.get(0).equals("osm")) {
                                    if (currentXMLTags.size() == 1) {
                                        events.addDatasetNode(osmFile, extractProperties(parser));
                                    } else if (currentXMLTags.size() > 1) {
                                        String tag = currentXMLTags.get(1);
                                        if (tag.equals("bounds")) {
                                            events.addDatasetBoundsNode(osmFile, extractProperties(parser));
                                        } else if (tag.equals("node")) {
                                            // Create OSMNode object with all attributes (but not tags)
                                            // <node id="269682538" lat="56.0420950"
                                            // lon="12.9693483" user="sanna" uid="31450"
                                            // visible="true" version="1" changeset="133823"
                                            // timestamp="2008-06-11T12:36:28Z"/>
                                            Map<String, Object> nodeProperties = extractProperties("node", parser, range);
                                            long osm_id = Long.parseLong(nodeProperties.get("node_osm_id").toString());
                                            events.addOSMNode(osm_id, nodeProperties);
                                        } else if (tag.equals("way")) {
                                            if (currentXMLTags.size() == 2) {
                                                // <way id="27359054" user="spull" uid="61533"
                                                // visible="true" version="8" changeset="4707351"
                                                // timestamp="2010-05-15T15:39:57Z">
                                                wayProperties = extractProperties("way", parser);
                                                wayNodes.clear();
                                            } else if (currentXMLTags.size() == 3 && currentXMLTags.get(2).equals("nd")) {
                                                Map<String, Object> properties = extractProperties(parser);
                                                wayNodes.add(Long.parseLong(properties.get("ref").toString()));
                                            }
                                        } else if (tag.equals("relation")) {
                                            if (currentXMLTags.size() == 2) {
                                                // <relation id="77965" user="Grillo" uid="13957"
                                                // visible="true" version="24" changeset="5465617"
                                                // timestamp="2010-08-11T19:25:46Z">
                                                relationProperties = extractProperties("relation", parser);
                                                relationMembers.clear();
                                            } else if (currentXMLTags.size() == 3 && currentXMLTags.get(2).equals("member")) {
                                                relationMembers.add(extractProperties(parser));
                                            }
                                        }
                                    }
                                }
                                depth++;
                                break;
                            case javax.xml.stream.XMLStreamConstants.END_ELEMENT:
                                if (currentXMLTags.size() == 2 && currentXMLTags.get(0).equals("osm")) {
                                    String tag = currentXMLTags.get(1);
                                    if (tag.equals("node")) {
                                        addOSMTags(events);
                                    } else if (tag.equals("way")) {
                                        long osm_id = Long.parseLong(wayProperties.get("way_osm_id").toString());
                                        events.addOSMWay(osm_id, wayProperties, wayNodes, currentNodeTags);
                                        addOSMTags(events);
                                    } else if (tag.equals("relation")) {
                                        long osm_id = Long.parseLong(relationProperties.get("relation_osm_id").toString());
                                        events.addOSMRelation(osm_id, relationProperties, relationMembers, currentNodeTags);
                                        addOSMTags(events);
                                    }
                                }
                                depth--;
                                currentXMLTags.remove(depth);
                                break;
                            default:
                                break;
                        }
                    } else {
                        break;
                    }
                } catch (XMLStreamException e) {
                    System.out.println("Failed to parse XML: " + e);
                    e.printStackTrace();
                    break;
                }
            }
            if (events.size() > config.batchSize() * 10) {
                System.out.println("Created unexpectedly large chunk: " + events.size());
            }
            return events.size() > 0;
        }

        private void addOSMTags(OSMInputChunk events) {
            if (currentNodeTags.size() > 0) {
                events.addOSMTags(currentNodeTags);
                currentNodeTags = new LinkedHashMap<>();
            }
            events.endTaggableEvent();
        }

        private InputStream openFile() throws IOException {
            FileInputStream input = new FileInputStream(osmFile);
            if (osmFile.endsWith(".bz2")) {
                return new BZip2CompressorInputStream(input);
            }else if (osmFile.endsWith(".gz")) {
                return new GzipCompressorInputStream(input);
            } else {
                return input;
            }
        }

        private XMLStreamReader getXMLParser() {
            try {
                javax.xml.stream.XMLInputFactory factory = javax.xml.stream.XMLInputFactory.newInstance();
                InputStreamReader reader = new InputStreamReader(openFile(), Charset.defaultCharset());
                return factory.createXMLStreamReader(reader);
            } catch (Exception e) {
                throw new RuntimeException("Failed to open XML: " + e.getMessage(), e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                this.parser.close();
            } catch (XMLStreamException e) {
                throw new IOException("Failed to close: " + e.getMessage(), e);
            }
        }
    }

    private abstract class MultiFileInputIterator implements InputIterator {
        private String[] osmFiles;
        private int currentFileIndex;
        private OSMInputIterator current;

        private MultiFileInputIterator(String[] osmFiles) {
            this.osmFiles = osmFiles;
            this.currentFileIndex = 0;
        }

        @Override
        public synchronized boolean next(InputChunk chunk) throws IOException {
            while (true) {
                if (current == null) {
                    if (!hasNextFile()) {
                        return false;
                    }
                    current = new OSMInputIterator(nextFile()) {
                        @Override
                        public InputChunk newChunk() {
                            throw new IllegalStateException("Inner OSMInputIterator should never be called directly");
                        }
                    };
                }

                if (current.next(chunk)) {
                    return true;
                }
                current.close();
                current = null;
            }
        }

        @Override
        public void close() {
            try {
                if (current != null) {
                    current.close();
                }
                current = null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private boolean hasNextFile() {
            return this.currentFileIndex < osmFiles.length;
        }

        private String nextFile() {
            String osmFile = osmFiles[currentFileIndex];
            currentFileIndex++;
            return osmFile;
        }
    }

    private class OSMNodesInputIterator extends MultiFileInputIterator {
        private OSMNodesInputIterator(String[] osmFiles) {
            super(osmFiles);
        }

        @Override
        public InputChunk newChunk() {
            return new OSMNodesInputChunk();
        }
    }

    private class OSMRelationshipsInputIterator extends MultiFileInputIterator {
        private OSMRelationshipsInputIterator(String[] osmFiles) {
            super(osmFiles);
        }

        @Override
        public InputChunk newChunk() {
            return new OSMRelationshipsInputChunk();
        }
    }

    public InputIterable nodes() {
        return () -> new OSMNodesInputIterator(osmFiles);
    }

    public InputIterable relationships() {
        return () -> new OSMRelationshipsInputIterator(osmFiles);
    }

    public IdMapper idMapper(NumberArrayFactory numberArrayFactory) {
        return IdMappers.strings(numberArrayFactory, groups);
    }

    public Collector badCollector() {
        return badCollector;
    }

    private static final int BYTES_PER_NODE = 1000;
    private static final int BYTES_PER_REL = 10000;

    private long calcFileSize() {
        long totalSize = 0;
        for (String osmFile : osmFiles) {
            long fileSize = FileUtils.size(fs, new File(osmFile));
            if (osmFile.endsWith(".bz2") || osmFile.endsWith(".gz")) fileSize *= 10;
            totalSize += fileSize;
        }
        return totalSize;
    }

    public Estimates calculateEstimates(ToIntFunction<Value[]> toIntFunction) throws IOException {
        long fileSize = calcFileSize();
        return Inputs.knownEstimates(fileSize / BYTES_PER_NODE, fileSize / BYTES_PER_REL, 8, 1, 8, 8, 1);
    }

    private Map<String, Object> extractProperties(XMLStreamReader parser) {
        return extractProperties(null, parser);
    }

    private Map<String, Object> extractProperties(String name, XMLStreamReader parser) {
        return extractProperties(name, parser, null);
    }

    private Map<String, Object> extractProperties(String name, XMLStreamReader parser, RangeFilter range) {
        // <node id="269682538" lat="56.0420950" lon="12.9693483" user="sanna"
        // uid="31450" visible="true" version="1" changeset="133823"
        // timestamp="2008-06-11T12:36:28Z"/>
        // <way id="27359054" user="spull" uid="61533" visible="true"
        // version="8" changeset="4707351" timestamp="2010-05-15T15:39:57Z">
        // <relation id="77965" user="Grillo" uid="13957" visible="true"
        // version="24" changeset="5465617" timestamp="2010-08-11T19:25:46Z">
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        for (int i = 0; i < parser.getAttributeCount(); i++) {
            String prop = parser.getAttributeLocalName(i);
            String value = parser.getAttributeValue(i);
            if (name != null && prop.equals("id")) {
                properties.put(name + "_osm_id", Long.parseLong(value));
            } else if (prop.equals("lat") || prop.equals("lon")) {
                properties.put(prop, Double.parseDouble(value));
            } else if (name != null && prop.equals("version")) {
                properties.put(prop, Integer.parseInt(value));
            } else if (prop.equals("visible")) {
                if (!value.equals("true") && !value.equals("1")) {
                    properties.put(prop, false);
                }
            } else if (prop.equals("timestamp")) {
                try {
                    LocalDateTime timestamp = LocalDateTime.parse(value, timestampFormat);
                    properties.put(prop, timestamp);
                } catch (DateTimeParseException e) {
                    error("Error parsing timestamp", e);
                }
            } else {
                properties.put(prop, value);
            }
        }
        if (properties.containsKey("lat") && properties.containsKey("lon")) {
            PointValue point = Values.pointValue(wgs84, (double) properties.get("lon"), (double) properties.get("lat"));
            if (range == null || range.withinRange(point.coordinate())) {
                properties.put("location", point);
            } else {
                //Nodes outside the filtered location should be completely ignored
                return null;
            }
        }
        return properties;
    }

    public interface RangeFilter {
        boolean withinRange(double[] coordinate);
    }

    private void error(String message) {
        System.err.println(message);
    }

    private void error(String message, Exception e) {
        System.err.println(message + ": " + e.getMessage());
    }
}
