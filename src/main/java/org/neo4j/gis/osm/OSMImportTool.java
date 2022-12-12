package org.neo4j.gis.osm;

import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.function.Predicates;
import org.neo4j.gis.osm.importer.OSMInput;
import org.neo4j.gis.osm.importer.PrintingImportLogicMonitor;
import org.neo4j.internal.batchimport.*;
import org.neo4j.internal.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.internal.helpers.Args;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.commons.lang3.ArrayUtils.isEmpty;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.*;
import static org.neo4j.internal.batchimport.input.BadCollector.BAD_FILE_NAME;
import static org.neo4j.internal.batchimport.input.Collectors.*;
import static org.neo4j.internal.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.internal.helpers.Strings.TAB;
import static org.neo4j.io.ByteUnit.bytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.kernel.impl.store.PropertyType.EMPTY_BYTE_ARRAY;

public class OSMImportTool {

    private static final String UNLIMITED = "unlimited";

    enum Options {
        HOME_DIR("into", null,
                "<home-dir>",
                "The root of the DBMS into which to do the import."),
        DB_NAME("database", null,
                "<database-name>",
                "Database name to import into. " + "Must not contain existing database.", true),
        DELETE_DB("delete", Boolean.FALSE, "<true/false>",
                "Whether or not to delete the existing database before creating a new one."),
        INPUT_ENCODING("input-encoding", null,
                "<character set>",
                "Character set that input data is encoded in. Provided value must be one out of the available "
                        + "character sets in the JVM, as provided by Charset#availableCharsets(). "
                        + "If no input encoding is provided, the default character set of the JVM will be used.",
                true),
        PROCESSORS("processors", null,
                "<max processor count>",
                "(advanced) Max number of processors used by the importer. Defaults to the number of "
                        + "available processors reported by the JVM"
                        + availableProcessorsHint()
                        + ". There is a certain amount of minimum threads needed so for that reason there "
                        + "is no lower bound for this value. For optimal performance this value shouldn't be "
                        + "greater than the number of available processors."),
        STACKTRACE("stacktrace", false,
                "<true/false>",
                "Enable printing of error stack traces."),
        BAD_TOLERANCE("bad-tolerance", UNLIMITED,
                "<max number of bad entries, or '" + UNLIMITED + "' for unlimited>",
                "Number of bad entries before the import is considered failed. This tolerance threshold is "
                        + "about relationships refering to missing nodes. Format errors in input data are "
                        + "still treated as errors"),
        SKIP_BAD_ENTRIES_LOGGING("skip-bad-entries-logging", Boolean.FALSE, "<true/false>",
                "Whether or not to skip logging bad entries detected during import."),
        SKIP_BAD_RELATIONSHIPS("skip-bad-relationships", Boolean.TRUE,
                "<true/false>",
                "Whether or not to skip importing relationships that refers to missing node ids, i.e. either "
                        + "start or end node id/group referring to node that wasn't specified by the "
                        + "node input data. "
                        + "Skipped nodes will be logged"
                        + ", containing at most number of entites specified by " + BAD_TOLERANCE.key() + ", unless "
                        + "otherwise specified by " + SKIP_BAD_ENTRIES_LOGGING.key() + " option."),
        RANGE("range", null,
                "<minx,miny,maxx,maxy>",
                "Optional filter for including only points within the specified range"
                ),
        SKIP_DUPLICATE_NODES("skip-duplicate-nodes", Boolean.FALSE,
                "<true/false>",
                "Whether or not to skip importing nodes that have the same id/group. In the event of multiple "
                        + "nodes within the same group having the same id, the first encountered will be imported "
                        + "whereas consecutive such nodes will be skipped. "
                        + "Skipped nodes will be logged"
                        + ", containing at most number of entities specified by " + BAD_TOLERANCE.key() + ", unless "
                        + "otherwise specified by " + SKIP_BAD_ENTRIES_LOGGING.key() + "option."),
        ADDITIONAL_CONFIG("additional-config", null,
                "<path/to/" + Config.DEFAULT_CONFIG_FILE_NAME + ">",
                "(advanced) File specifying database-specific configuration. For more information consult "
                        + "manual about available configuration options for a neo4j configuration file. "
                        + "Only configuration affecting store at time of creation will be read.", true),
        MAX_MEMORY("max-memory", null,
                "<max memory that importer can use>",
                "(advanced) Maximum memory that importer can use for various data structures and caching " +
                        "to improve performance. If left as unspecified (null) it is set to " + DEFAULT_MAX_MEMORY_PERCENT +
                        "% of (free memory on machine - max JVM memory). " +
                        "Values can be plain numbers, like 10000000 or e.g. 20G for 20 gigabyte, or even e.g. 70%."),
        CACHE_ON_HEAP("cache-on-heap", null,
                "Whether or not to allow allocating memory for the cache on heap",
                "(advanced) Whether or not to allow allocating memory for the cache on heap. " +
                        "If 'false' then caches will still be allocated off-heap, but the additional free memory " +
                        "inside the JVM will not be allocated for the caches. This to be able to have better control " +
                        "over the heap memory"),
        HIGH_IO("high-io", null, "Assume a high-throughput storage subsystem",
                "(advanced) Ignore environment-based heuristics, and assume that the target storage subsystem can " +
                        "support parallel IO with high throughput."),
        DETAILED_PROGRESS("detailed-progress", Boolean.FALSE, "true/false", "Use the old detailed 'spectrum' progress printing"),
        TRACE_PAGE_CACHE("trace-page-cache", Boolean.FALSE, "true/false", "Trace the counts of page cache usage");

        private final String key;
        private final Object defaultValue;
        private final String usage;
        private final String description;
        private final boolean keyAndUsageGoTogether;
        private final boolean supported;

        Options(String key, Object defaultValue, String usage, String description) {
            this(key, defaultValue, usage, description, false, false);
        }

        Options(String key, Object defaultValue, String usage, String description, boolean supported) {
            this(key, defaultValue, usage, description, supported, false);
        }

        Options(String key, Object defaultValue, String usage, String description, boolean supported, boolean keyAndUsageGoTogether) {
            this.key = key;
            this.defaultValue = defaultValue;
            this.usage = usage;
            this.description = description;
            this.supported = supported;
            this.keyAndUsageGoTogether = keyAndUsageGoTogether;
        }

        String key() {
            return key;
        }

        String argument() {
            return "--" + key();
        }

        void printUsage(PrintStream out) {
            out.println(argument() + spaceInBetweenArgumentAndUsage() + usage);
            for (String line : splitLongLine(descriptionWithDefaultValue().replace("`", ""), 80)) {
                out.println("\t" + line);
            }
        }

        private String spaceInBetweenArgumentAndUsage() {
            return keyAndUsageGoTogether ? "" : " ";
        }

        String descriptionWithDefaultValue() {
            String result = description;
            if (defaultValue != null) {
                if (!result.endsWith(".")) {
                    result += ".";
                }
                result += " Default value: " + defaultValue;
            }
            return result;
        }

        String manPageEntry() {
            String filteredDescription = descriptionWithDefaultValue().replace(availableProcessorsHint(), "");
            String usageString = (usage.length() > 0) ? spaceInBetweenArgumentAndUsage() + usage : "";
            return "*" + argument() + usageString + "*::\n" + filteredDescription + "\n\n";
        }

        String manualEntry() {
            return "[[import-tool-option-" + key() + "]]\n" + manPageEntry() + "//^\n\n";
        }

        Object defaultValue() {
            return defaultValue;
        }

        private static String availableProcessorsHint() {
            return " (in your case " + Runtime.getRuntime().availableProcessors() + ")";
        }

        public boolean isSupportedOption() {
            return this.supported;
        }
    }

    public static void main(String[] incomingArguments) throws Exception {
        main(incomingArguments, false);
    }

    public static final Validator<File> DIRECTORY_IS_WRITABLE = value ->
    {
        if (value.mkdirs()) {   // It's OK, we created the directory right now, which means we have write access to it
            return;
        }

        File test = new File(value, "_______test___");
        try {
            test.createNewFile();
        } catch (IOException e) {
            throw new IllegalArgumentException("Directory '" + value + "' not writable: " + e.getMessage());
        } finally {
            test.delete();
        }
    };

    public static void main(String[] incomingArguments, boolean defaultSettingsSuitableForTests) throws Exception {
        PrintStream out = System.out;
        PrintStream err = System.err;
        Args args = Args.parse(incomingArguments);

        if (isEmpty(incomingArguments) || asksForUsage(args)) {
            printUsage(out);
            return;
        }

        String[] osmFiles;
        boolean enableStacktrace;
        Number processors;
        long badTolerance;
        boolean skipBadRelationships;
        boolean skipDuplicateNodes;
        boolean skipBadEntriesLogging;
        Config dbConfig;
        OutputStream badOutput = null;
        org.neo4j.internal.batchimport.Configuration configuration;
        File badFile = null;
        Boolean defaultHighIO;
        InputStream in;

        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            File homeDir = args.interpretOption(Options.HOME_DIR.key(), Converters.mandatory(), Converters.toFile(), DIRECTORY_IS_WRITABLE);
            homeDir.mkdirs();
            var homeLayout = Neo4jLayout.of(homeDir.toPath());
            var databaseName = args.get(Options.DB_NAME.key(), "osm");
            var databaseLayout = homeLayout.databaseLayout(databaseName);
            boolean deleteDb = args.getBoolean(Options.DELETE_DB.key(), Boolean.FALSE, Boolean.TRUE);
            if (deleteDb) {
                FileUtils.deleteDirectory(databaseLayout.databaseDirectory());
                FileUtils.deleteDirectory(databaseLayout.getTransactionLogsDirectory());
            }
            Config config = Config.defaults(GraphDatabaseSettings.neo4j_home, Path.of(homeDir.getAbsolutePath()));
            Path logsDir = config.get(GraphDatabaseSettings.logs_directory);
            fs.mkdirs(logsDir);

            skipBadEntriesLogging = args.getBoolean(Options.SKIP_BAD_ENTRIES_LOGGING.key(), (Boolean) Options.SKIP_BAD_ENTRIES_LOGGING.defaultValue(), false);
            if (!skipBadEntriesLogging) {
                badFile = new File(homeDir, BAD_FILE_NAME);
                badOutput = new BufferedOutputStream(fs.openAsOutputStream(badFile.toPath(), false));
            }
            OSMRange range = args.interpretOption(Options.RANGE.key(), Converters.optional(), Converters.toRange(), RANGE_IS_VALID);
            osmFiles = args.orphansAsArray();
            if (osmFiles.length == 0) {
                throw new IllegalArgumentException("No OSM files specified");
            }

            enableStacktrace = args.getBoolean(Options.STACKTRACE.key(), Boolean.FALSE, Boolean.TRUE);
            processors = args.getNumber(Options.PROCESSORS.key(), null);
            badTolerance = parseNumberOrUnlimited(args, Options.BAD_TOLERANCE);

            skipBadRelationships = args.getBoolean(Options.SKIP_BAD_RELATIONSHIPS.key(), (Boolean) Options.SKIP_BAD_RELATIONSHIPS.defaultValue(), true);
            skipDuplicateNodes = args.getBoolean(Options.SKIP_DUPLICATE_NODES.key(), (Boolean) Options.SKIP_DUPLICATE_NODES.defaultValue(), true);
            defaultHighIO = args.getBoolean(Options.HIGH_IO.key(), (Boolean) Options.HIGH_IO.defaultValue(), true);

            Collector badCollector = getBadCollector(badTolerance, skipBadRelationships, skipDuplicateNodes, skipBadEntriesLogging, badOutput);

            dbConfig = loadDbConfig(args.interpretOption(Options.ADDITIONAL_CONFIG.key(), Converters.optional(), Converters.toFile(), f -> Validators.REGEX_FILE_EXISTS.validate(f.getAbsolutePath())));
            configuration = importConfiguration(processors, defaultSettingsSuitableForTests, homeDir, defaultHighIO);
            in = defaultSettingsSuitableForTests ? new ByteArrayInputStream(EMPTY_BYTE_ARRAY) : System.in;
            boolean detailedProgress = args.getBoolean(Options.DETAILED_PROGRESS.key(), (Boolean) Options.DETAILED_PROGRESS.defaultValue());
            boolean tracePageCache = args.getBoolean(Options.TRACE_PAGE_CACHE.key(), (Boolean) Options.TRACE_PAGE_CACHE.defaultValue());
            doImport(out, err, in, databaseLayout, logsDir.toFile(), badFile, fs, osmFiles, enableStacktrace, dbConfig, badOutput, badCollector, configuration, detailedProgress, tracePageCache, range);
        }
    }

    static class OSMRange implements OSMInput.RangeFilter {
        double[] range = new double[4];
        ArrayList<String> errors = new ArrayList<>();

        OSMRange(String rangeSpec) {
            if (rangeSpec == null) {
                errors.add("Range was null");
            } else {
                String[] fields = rangeSpec.split("\\s*[\\,\\;]\\s*");
                if (fields.length == 4) {
                    for (int i = 0; i < range.length; i++) {
                        try {
                            range[i] = Double.parseDouble(fields[i]);
                        } catch (Exception e) {
                            errors.add("Failed to parse double from field[" + i + "]: '" + fields[i] + "'");
                        }
                    }
                } else {
                    errors.add("Did not have exactly four fields in '" + rangeSpec + "'");
                }
            }
        }

        public boolean withinRange(double[] coordinate) {
            return coordinate[0] >= range[0] && coordinate[1] >= range[1] && coordinate[0] <= range[2] && coordinate[1] <= range[3];
        }

        boolean isValid() {
            return errors.size() == 0;
        }

        String error() {
            return Arrays.toString(errors.toArray());
        }
    }

    static class Converters {
        public static <T> Function<String, T> mandatory() {
            return key ->
            {
                throw new IllegalArgumentException("Missing argument '" + key + "'");
            };
        }

        public static <T> Function<String, T> optional() {
            return from -> null;
        }

        public static Function<String, File> toFile() {
            return File::new;
        }

        public static Function<String, OSMRange> toRange() {
            return OSMRange::new;
        }
    }

    public static final Validator<OSMRange> RANGE_IS_VALID = value -> {
        if (!value.isValid()) {
            throw new IllegalArgumentException("Range '" + value + "' is not valid: " + value.error());
        }
    };

    public static void doImport(PrintStream out, PrintStream err, InputStream in, DatabaseLayout databaseLayout, File logsDir, File badFile,
                                FileSystemAbstraction fs, String[] osmFiles,
                                boolean enableStacktrace,
                                Config dbConfig, OutputStream badOutput,
                                Collector badCollector, Configuration configuration,
                                boolean detailedProgress, boolean tracePageCache,
                                OSMRange range) throws IOException {
        boolean success;
        LifeSupport life = new LifeSupport();

        InternalLogProvider lp = NullLogProvider.getInstance();
        LogService logService = life.add(new SimpleLogService(lp));

        final JobScheduler jobScheduler = life.add(createScheduler());

        life.start();
        ExecutionMonitor executionMonitor = detailedProgress
                ? new SpectrumExecutionMonitor(2, TimeUnit.SECONDS, out, SpectrumExecutionMonitor.DEFAULT_WIDTH)
                : ExecutionMonitors.defaultVisible();
        Monitor importMonitor = new PrintingImportLogicMonitor(out, err);
        var cacheTracer = tracePageCache ? new DefaultPageCacheTracer() : PageCacheTracer.NULL;
        BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate(
                databaseLayout,
                fs,
                cacheTracer,
                configuration,
                logService,
                executionMonitor,
                EMPTY,
                new EmptyLogTailMetadata(),
                dbConfig,
                importMonitor,
                jobScheduler,
                badCollector,
                TransactionLogInitializer.getLogFilesInitializer(),
                IndexImporterFactory.EMPTY,
                EmptyMemoryTracker.INSTANCE,
                CursorContextFactory.NULL_CONTEXT_FACTORY
        );
        printOverview(databaseLayout.databaseDirectory(), osmFiles, configuration, out);
        success = false;
        try {
            importer.doImport(new OSMInput(fs, osmFiles, configuration, range));
            success = true;
        } catch (Exception e) {
            throw andPrintError("Import error", e, enableStacktrace, err);
        } finally {
            long numberOfBadEntries = badCollector.badEntries();
            badCollector.close();
            IOUtils.closeAll(badOutput);

            if (badFile != null) {
                if (numberOfBadEntries > 0) {
                    System.out.println("There were bad entries which were skipped and logged into " +
                            badFile.getAbsolutePath());
                }
            }

            life.shutdown();

            if (tracePageCache) {
                System.out.println("Page cache counts:");
                System.out.println("  faults:  " + cacheTracer.faults());
                System.out.println("  pins:    " + cacheTracer.pins());
                System.out.println("  unpins:  " + cacheTracer.unpins());
                System.out.println("  hits:    " + cacheTracer.hits());
                System.out.println("  flushes: " + cacheTracer.flushes());
            }

            if (!success) {
                err.println("WARNING Import failed. The store files in " + databaseLayout.databaseDirectory().toAbsolutePath() +
                        " are left as they are, although they are likely in an unusable state. " +
                        "Starting a database on these store files will likely fail or observe inconsistent records so " +
                        "start at your own risk or delete the store manually");
            }
        }
    }

    public static org.neo4j.internal.batchimport.Configuration importConfiguration(
            Number processors, boolean defaultSettingsSuitableForTests, File homeDir, Boolean defaultHighIO) {
        return new org.neo4j.internal.batchimport.Configuration() {
            @Override
            public long pageCacheMemory() {
                return defaultSettingsSuitableForTests ? mebiBytes(8) : DEFAULT.pageCacheMemory();
            }

            @Override
            public int maxNumberOfWorkerThreads() {
                return processors != null ? processors.intValue() : DEFAULT.maxNumberOfWorkerThreads();
            }

            @Override
            public boolean highIO() {
                return defaultHighIO != null ? defaultHighIO : DEFAULT.highIO();
            }
        };
    }

    private static String manualReference(ManualPage page, Anchor anchor) {
        // Docs are versioned major.minor-suffix, so drop the patch version.
        String[] versionParts = Version.getNeo4jVersion().split("-");
        versionParts[0] = versionParts[0].substring(0, 3);
        String docsVersion = String.join("-", versionParts);

        return " https://neo4j.com/docs/operations-manual/" + docsVersion + "/" + page.getReference(anchor);
    }

    /**
     * Method name looks strange, but look at how it's used and you'll see why it's named like that.
     *
     * @param stackTrace whether or not to also print the stack trace of the error.
     * @param err
     */
    private static RuntimeException andPrintError(String typeOfError, Exception e, boolean stackTrace, PrintStream err) {
        // List of common errors that can be explained to the user
        if (DuplicateInputIdException.class.equals(e.getClass())) {
            printErrorMessage("Duplicate input ids that would otherwise clash can be put into separate id space, " +
                            "read more about how to use id spaces in the manual:" +
                            manualReference(ManualPage.IMPORT_TOOL_FORMAT, Anchor.ID_SPACES), e, stackTrace,
                    err);
        } else if (Exceptions.contains(e, Predicates.instanceOfAny(InputException.class))) {
            printErrorMessage("Error in input data", e, stackTrace, err);
        }
        // Fallback to printing generic error and stack trace
        else {
            printErrorMessage(typeOfError + ": " + e.getMessage(), e, true, err);
        }
        err.println();

        // Mute the stack trace that the default exception handler would have liked to print.
        // Calling System.exit( 1 ) or similar would be convenient on one hand since we can set
        // a specific exit code. On the other hand It's very inconvenient to have any System.exit
        // call in code that is tested.
        Thread.currentThread().setUncaughtExceptionHandler((t, e1) ->
        {
            /* Shhhh */
        });
        throwIfUnchecked(e);
        return new RuntimeException(e); // throw in order to have process exit with !0
    }

    private static void printErrorMessage(String string, Exception e, boolean stackTrace, PrintStream err) {
        err.println(string);
        err.println("Caused by:" + e.getMessage());
        if (stackTrace) {
            e.printStackTrace(err);
        }
    }

    private static Collector getBadCollector(long badTolerance, boolean skipBadRelationships, boolean skipDuplicateNodes, boolean skipBadEntriesLogging, OutputStream badOutput) {
        int collect = collect(skipBadRelationships, skipDuplicateNodes, false);
        return skipBadEntriesLogging ? silentBadCollector(badTolerance, collect) : badCollector(badOutput, badTolerance, collect);
    }

    private static long parseNumberOrUnlimited(Args args, Options option) {
        String value = args.get(option.key(), option.defaultValue().toString());
        return UNLIMITED.equals(value) ? BadCollector.UNLIMITED_TOLERANCE : Long.parseLong(value);
    }

    private static Config loadDbConfig(File file) {
        return file != null && file.exists() ? Config.newBuilder().fromFile(file.toPath()).build() : Config.defaults();
    }

    private static void printOverview(Path storeDir, String[] osmFiles, org.neo4j.internal.batchimport.Configuration configuration, PrintStream out) {
        out.println("Neo4j version: " + Version.getNeo4jVersion());
        out.println("Importing the contents of these OSM files into " + storeDir + ":");
        for (String file : osmFiles) {
            printIndented(file, out);
        }
        out.println();
        out.println("Available resources:");
        printIndented("Total machine memory: " + bytes(OsBeanUtil.getTotalPhysicalMemory()), out);
        printIndented("Free machine memory: " + bytes(OsBeanUtil.getFreePhysicalMemory()), out);
        printIndented("Max heap memory : " + bytes(Runtime.getRuntime().maxMemory()), out);
        printIndented("Processors: " + configuration.maxNumberOfWorkerThreads(), out);
        printIndented("Configured max page memory: " + bytes(configuration.pageCacheMemory()), out);
        printIndented("High-IO: " + configuration.highIO(), out);
        out.println();
    }

    private static void printIndented(Object value, PrintStream out) {
        out.println("  " + value);
    }

    private static void printUsage(PrintStream out) {
        out.println("Neo4j OpenStreetMap Import Tool");
        for (String line : splitLongLine("osm-import is used to create a new Neo4j database from data in OSM XML files.", 80)) {
            out.println("\t" + line);
        }
        out.println("Usage:");
        for (Options option : Options.values()) {
            option.printUsage(out);
        }

        out.println("Example:");
        out.println(TAB + "bin/osm-import --into osm.db sweden-latest.osm.bz2 denmark-latest.osm norway-latest.osm ");
    }

    private static boolean asksForUsage(Args args) {
        for (String orphan : args.orphans()) {
            if (isHelpKey(orphan)) {
                return true;
            }
        }

        for (Map.Entry<String, String> option : args.asMap().entrySet()) {
            if (isHelpKey(option.getKey())) {
                return true;
            }
        }
        return args.orphans().size() == 0;
    }

    private static boolean isHelpKey(String key) {
        return key.equals("?") || key.equals("help");
    }

    private enum ManualPage {
        IMPORT_TOOL_FORMAT("tools/import/file-header-format/");

        private final String page;

        ManualPage(String page) {
            this.page = page;
        }

        public String getReference(Anchor anchor) {
            // As long as the the operations manual is single-page we only use the anchor.
            return page + "#" + anchor.anchor;
        }
    }

    /*
     * Copied from org.neo4j.internal.helpers.Args, which had this method up to Neo4j 4.1, but removed in 4.2.
     */
    public static String[] splitLongLine( String description, int maxLength )
    {
        List<String> lines = new ArrayList<>();
        while ( !description.isEmpty() )
        {
            String line = description.substring( 0, Math.min( maxLength, description.length() ) );
            int position = line.indexOf( '\n' );
            if ( position > -1 )
            {
                line = description.substring( 0, position );
                lines.add( line );
                description = description.substring( position );
                if ( !description.isEmpty() )
                {
                    description = description.substring( 1 );
                }
            }
            else
            {
                position = description.length() > maxLength ?
                        findSpaceBefore( description, maxLength ) : description.length();
                line = description.substring( 0, position );
                lines.add( line );
                description = description.substring( position );
            }
        }
        return lines.toArray( new String[0] );
    }

    private static int findSpaceBefore( String description, int position )
    {
        while ( !Character.isWhitespace( description.charAt( position ) ) )
        {
            position--;
        }
        return position + 1;
    }

    private enum Anchor {
        ID_SPACES("import-tool-id-spaces"),
        RELATIONSHIP("import-tool-header-format-rels");

        private final String anchor;

        Anchor(String anchor) {
            this.anchor = anchor;
        }
    }
}
