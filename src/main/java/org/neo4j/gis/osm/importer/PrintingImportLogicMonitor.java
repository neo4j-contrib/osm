package org.neo4j.gis.osm.importer;

import org.neo4j.unsafe.impl.batchimport.ImportLogic;

import java.io.PrintStream;

import static org.neo4j.helpers.Format.bytes;

public class PrintingImportLogicMonitor implements ImportLogic.Monitor {
    private final PrintStream out;
    private final PrintStream err;

    public PrintingImportLogicMonitor(PrintStream out, PrintStream err) {
        this.out = out;
        this.err = err;
    }

    @Override
    public void doubleRelationshipRecordUnitsEnabled() {
        out.println("Will use double record units for all relationships");
    }

    @Override
    public void mayExceedNodeIdCapacity(long capacity, long estimatedCount) {
        err.printf("WARNING: estimated number of relationships %d may exceed capacity %d of selected record format%n",
                estimatedCount, capacity);
    }

    @Override
    public void mayExceedRelationshipIdCapacity(long capacity, long estimatedCount) {
        err.printf("WARNING: estimated number of nodes %d may exceed capacity %d of selected record format%n",
                estimatedCount, capacity);
    }

    @Override
    public void insufficientHeapSize(long optimalMinimalHeapSize, long heapSize) {
        err.printf("WARNING: heap size %s may be too small to complete this import. Suggested heap size is %s",
                bytes(heapSize), bytes(optimalMinimalHeapSize));
    }

    @Override
    public void abundantHeapSize(long optimalMinimalHeapSize, long heapSize) {
        err.printf("WARNING: heap size %s is unnecessarily large for completing this import.%n" +
                        "The abundant heap memory will leave less memory for off-heap importer caches. Suggested heap size is %s",
                bytes(heapSize), bytes(optimalMinimalHeapSize));
    }

    @Override
    public void insufficientAvailableMemory(long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory) {
        err.printf("WARNING: %s memory may not be sufficient to complete this import. Suggested memory distribution is:%n" +
                        "heap size: %s%n" +
                        "minimum free and available memory excluding heap size: %s",
                bytes(availableMemory), bytes(optimalMinimalHeapSize), bytes(estimatedCacheSize));
    }
}
