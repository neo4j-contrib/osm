package org.neo4j.gis.osm.importer;

import java.io.*;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.*;
import org.neo4j.values.storable.Value;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class OSMInput implements Input {
    private final File osmFile;
    private final Groups groups = new Groups();
    private final Group nodesGroup;
    private final Group waysGroup;
    private final Group tagsGroup;
    private final Collector badCollector;
    private final FileSystemAbstraction fs;

    public OSMInput(FileSystemAbstraction fs, File osmFile, Collector badCollector) {
        this.fs = fs;
        this.osmFile = osmFile;
        this.badCollector = badCollector;
        nodesGroup = this.groups.getOrCreate("osm_nodes");
        waysGroup = this.groups.getOrCreate("osm_ways");
        tagsGroup = this.groups.getOrCreate("osm_tags");
    }

    private XMLStreamReader parser() {
        try {
            javax.xml.stream.XMLInputFactory factory = javax.xml.stream.XMLInputFactory.newInstance();
            InputStreamReader reader = new InputStreamReader(new FileInputStream(osmFile), Charset.defaultCharset());
            return factory.createXMLStreamReader(reader);
        } catch (Exception e) {
            throw new RuntimeException("Failed to open XML: " + e.getMessage(), e);
        }
    }

    private class NodeEvent {
        String label;
        String osmId;
        Group group;
        Map<String, Object> properties;

        private NodeEvent(String label, String osmId, Group group, Map<String, Object> properties) {
            this.label = label;
            this.osmId = osmId;
            this.group = group;
            this.properties = properties;
        }
    }

    private class OSMNode extends NodeEvent {
        private OSMNode(long id, Map<String, Object> properties) {
            super("OSMNode", "n" + id, nodesGroup, properties);
        }
    }

    private class OSMWay extends NodeEvent {
        private OSMWay(long id, Map<String, Object> properties) {
            super("OSMWay", "w" + id, waysGroup, properties);
        }
    }

    private class OSMTags extends NodeEvent {
        private OSMTags(String id, Map<String, Object> properties) {
            super("OSMTags", "t" + id, tagsGroup, properties);
        }
    }

    interface OSMInputChunk extends InputChunk {
        void addOSMNode(long id, Map<String, Object> properties);

        void addOSMWay(long id, Map<String, Object> properties);

        void addOSMTags(Map<String, Object> properties);

        long size();

        void reset();
    }

    class OSMNodesInputChunk implements OSMInputChunk {
        NodeEvent[] data = new NodeEvent[CHUNK_SIZE];
        int eventsAdded = 0;
        int currentRead = -1;

        private void addEvent(NodeEvent event) {
            data[eventsAdded] = event;
            eventsAdded++;
        }

        @Override
        public void addOSMNode(long id, Map<String, Object> properties) {
            addEvent(new OSMNode(id, properties));
        }

        @Override
        public void addOSMWay(long id, Map<String, Object> properties) {
            addEvent(new OSMWay(id, properties));
        }

        @Override
        public void addOSMTags(Map<String, Object> properties) {
            addEvent(new OSMTags(data[eventsAdded - 1].osmId, properties));
        }

        @Override
        public long size() {
            return eventsAdded;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            currentRead++;
            if (data != null && currentRead < eventsAdded) {
                // Make OSM node
                NodeEvent event = data[currentRead];
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
            this.eventsAdded = 0;
            this.currentRead = -1;
        }

        @Override
        public void close() {
            reset();
            this.data = null;
        }
    }

    private class RelationshipEvent {
        String type;
        String fromId;
        String toId;
        Group fromGroup;
        Group toGroup;

        private RelationshipEvent(String type, String fromId, Group fromGroup, String toId, Group toGroup) {
            this.type = type;
            this.fromId = fromId;
            this.toId = toId;
            this.fromGroup = fromGroup;
            this.toGroup = toGroup;
        }
    }

    private class OSMTagsRel extends RelationshipEvent {
        private OSMTagsRel(NodeEvent from, NodeEvent to) {
            super("TAGS", from.osmId, from.group, to.osmId, to.group);
        }
    }

    class OSMRelationshipsInputChunk implements OSMInputChunk {
        RelationshipEvent[] data = new RelationshipEvent[CHUNK_SIZE];
        NodeEvent previousNodeEvent = null;
        int eventsAdded = 0;
        int currentRead = -1;

        private void addEvent(RelationshipEvent event) {
            data[eventsAdded] = event;
            eventsAdded++;
        }

        @Override
        public void addOSMNode(long id, Map<String, Object> properties) {
            previousNodeEvent = new OSMNode(id, properties);
        }

        @Override
        public void addOSMWay(long id, Map<String, Object> properties) {
            previousNodeEvent = new OSMWay(id, properties);
        }

        @Override
        public void addOSMTags(Map<String, Object> properties) {
            NodeEvent tagNode = new OSMTags(previousNodeEvent.osmId, properties);
            OSMTagsRel tagsRel = new OSMTagsRel(previousNodeEvent, tagNode);
            System.out.println("Creating relationship: (" + tagsRel.fromId + ")-[" + tagsRel.type + "]->(" + tagsRel.toId + ")");
            addEvent(tagsRel);
        }

        @Override
        public long size() {
            return eventsAdded;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            currentRead++;
            if (data != null && currentRead < eventsAdded) {
                // Make relationship between two nodes, with nodeId mapped using groups
˚                RelationshipEvent event = data[currentRead];
                visitor.startId(event.fromId, event.fromGroup);
                visitor.endId(event.toId, event.toGroup);
                visitor.type(event.type);
                visitor.endOfEntity();
                return true;
            }
            return false;
        }

        @Override
        public void reset() {
            this.eventsAdded = 0;
            this.currentRead = -1;
        }

        @Override
        public void close() {
            reset();
            this.data = null;
        }
    }

    // "2008-06-11T12:36:28Z"
    private DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private static final int CHUNK_SIZE = 1000;

    private abstract class OSMInputIterator implements InputIterator {
        private final XMLStreamReader parser;
        private boolean startedWays = false;
        private final ArrayList<Long> wayNodes = new ArrayList<>();
        private Map<String, Object> wayProperties = null;
        private int depth = 0;
        private ArrayList<String> currentXMLTags = new ArrayList<>();
        private LinkedHashMap<String, Object> currentNodeTags = new LinkedHashMap<>();

        private OSMInputIterator(XMLStreamReader parser) {
            this.parser = parser;
        }

        @Override
        public synchronized boolean next(InputChunk chunk) {
            OSMInputChunk events = (OSMInputChunk) chunk;
            events.reset();
            while (events.size() < CHUNK_SIZE) {
                if (currentXMLTags.size() > 0 && !currentXMLTags.get(0).equals("osm")) {
                    System.out.println("Whaaaat!");
                }
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
                                    //TODO: should we store dataset wide properties like bounds?
                                    if (currentXMLTags.size() > 1) {
                                        if (currentXMLTags.get(1).equals("node")) {
                                            // Create OSMNode object with all attributes (but not tags)
                                            // <node id="269682538" lat="56.0420950"
                                            // lon="12.9693483" user="sanna" uid="31450"
                                            // visible="true" version="1" changeset="133823"
                                            // timestamp="2008-06-11T12:36:28Z"/>
                                            Map<String, Object> nodeProperties = extractProperties("node", parser);
                                            long osm_id = Long.parseLong(nodeProperties.get("node_osm_id").toString());
                                            events.addOSMNode(osm_id, nodeProperties);
                                        } else if (currentXMLTags.get(1).equals("way")) {
                                            if (currentXMLTags.size() == 2) {
                                                // <way id="27359054" user="spull" uid="61533"
                                                // visible="true" version="8" changeset="4707351"
                                                // timestamp="2010-05-15T15:39:57Z">
                                                if (!startedWays) {
                                                    startedWays = true;
                                                }
                                                wayProperties = extractProperties("way", parser);
                                                wayNodes.clear();
                                            } else if (currentXMLTags.size() == 3 && currentXMLTags.get(2).equals("nd")) {
                                                Map<String, Object> properties = extractProperties(parser);
                                                wayNodes.add(Long.parseLong(properties.get("ref").toString()));
                                            }
                                        }
                                    }
                                }
                                depth++;
                                break;
                            case javax.xml.stream.XMLStreamConstants.END_ELEMENT:
                                if (currentXMLTags.size() == 2 && currentXMLTags.get(0).equals("osm")) {
                                    if (currentXMLTags.get(1).equals("node")) {
                                        if (currentNodeTags.size() > 0) {
                                            events.addOSMTags(currentNodeTags);
                                            currentNodeTags = new LinkedHashMap<>();
                                        }
                                    } else if (currentXMLTags.get(1).equals("way")) {
                                        long osm_id = Long.parseLong(wayProperties.get("way_osm_id").toString());
                                        events.addOSMWay(osm_id, wayProperties);
                                        if (currentNodeTags.size() > 0) {
                                            events.addOSMTags(currentNodeTags);
                                            currentNodeTags = new LinkedHashMap<>();
                                        }
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
                    e.printStackTrace();
                    break;
                }
            }
            return events.size() > 0;
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

    private class OSMNodesInputIterator extends OSMInputIterator {
        private OSMNodesInputIterator(XMLStreamReader parser) {
            super(parser);
        }

        @Override
        public InputChunk newChunk() {
            return new OSMNodesInputChunk();
        }
    }

    private class OSMRelationshipsInputIterator extends OSMInputIterator {
        private OSMRelationshipsInputIterator(XMLStreamReader parser) {
            super(parser);
        }

        @Override
        public InputChunk newChunk() {
            return new OSMRelationshipsInputChunk();
        }
    }

    public InputIterable nodes() {
        return InputIterable.replayable(() -> new OSMNodesInputIterator(parser()));
    }

    public InputIterable relationships() {
        return InputIterable.replayable(() -> new OSMRelationshipsInputIterator(parser()));
    }

    public IdMapper idMapper(NumberArrayFactory numberArrayFactory) {
        return IdMappers.strings(numberArrayFactory, groups);
    }

    public Collector badCollector() {
        return badCollector;
    }

    private static final int BYTES_PER_NODE = 1000;
    private static final int BYTES_PER_REL = 10000;

    private long calcFileSize(File osmFile) {
        return FileUtils.size(fs, osmFile);
    }

    public Estimates calculateEstimates(ToIntFunction<Value[]> toIntFunction) throws IOException {
        long fileSize = calcFileSize(osmFile);
        return Inputs.knownEstimates(fileSize / BYTES_PER_NODE, fileSize / BYTES_PER_REL, 8, 1, 8, 8, 1);
    }

    private Map<String, Object> extractProperties(XMLStreamReader parser) {
        return extractProperties(null, parser);
    }

    private Map<String, Object> extractProperties(String name,
                                                  XMLStreamReader parser) {
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
                prop = name + "_osm_id";
                name = null;
            }
            if (prop.equals("lat") || prop.equals("lon")) {
                properties.put(prop, Double.parseDouble(value));
            } else if (name != null && prop.equals("version")) {
                properties.put(prop, Integer.parseInt(value));
            } else if (prop.equals("visible")) {
                if (!value.equals("true") && !value.equals("1")) {
                    properties.put(prop, false);
                }
            } else if (prop.equals("timestamp")) {
                try {
                    Date timestamp = timestampFormat.parse(value);
                    properties.put(prop, timestamp.getTime());
                } catch (ParseException e) {
                    error("Error parsing timestamp", e);
                }
            } else {
                properties.put(prop, value);
            }
        }
        if (name != null) {
            properties.put("name", name);
        }
        return properties;
    }

    private void error(String message, Exception e) {
        System.err.println(message + ": " + e.getMessage());
    }
}
