package org.neo4j.gis.osm;

import org.neo4j.gis.osm.importer.OSMInput;
import org.neo4j.gis.osm.importer.PrintingImportLogicMonitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.batchimport.staging.SpectrumExecutionMonitor;

import java.io.*;
import java.util.concurrent.TimeUnit;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.badCollector;

public class OSMImportTool {

    private final File storeDir;
    private final File osmFile;

    public OSMImportTool(String osmFile, String storeDir) {
        this.osmFile = new File(osmFile);
        this.storeDir = new File(storeDir);
    }

    public static void main(String[] args) throws Exception {
        File osmFile = new File(args[0]);
        File storeDir = new File(args[1]);
        FileUtils.deleteRecursively(storeDir);
        storeDir.mkdirs();
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction() ) {
            new OSMImportTool(osmFile.getCanonicalPath(), storeDir.getCanonicalPath()).run(fs);
        }
    }

    public void run(FileSystemAbstraction fs) throws IOException {
        boolean detailedProgress = true;
        PrintStream out = System.out;
        PrintStream err = System.err;
        InputStream in = System.in;
        Configuration config = DEFAULT;
        storeDir.mkdirs();
        LifeSupport life = new LifeSupport();
        Config dbConfig = Config.defaults();
        File internalLogFile = dbConfig.get(store_internal_log_path);
        LogService logService = life.add(StoreLogService.withInternalLog(internalLogFile).build(fs));
        final CentralJobScheduler jobScheduler = life.add(new CentralJobScheduler());

        ExecutionMonitor executionMonitor = detailedProgress
                ? new SpectrumExecutionMonitor(2, TimeUnit.SECONDS, out, SpectrumExecutionMonitor.DEFAULT_WIDTH)
                : ExecutionMonitors.defaultVisible(in, jobScheduler);
        BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate(storeDir,
                fs,
                null, // no external page cache
                config,
                logService, executionMonitor,
                EMPTY,
                dbConfig,
                RecordFormatSelector.selectForConfig(dbConfig, logService.getInternalLogProvider()),
                new PrintingImportLogicMonitor(out, err));
        importer.doImport(new OSMInput(fs, osmFile, badCollector(new FileOutputStream("bad.log"), -1)));
    }

}
