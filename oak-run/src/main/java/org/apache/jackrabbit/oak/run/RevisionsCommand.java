package org.apache.jackrabbit.oak.run;

import com.google.common.io.Closer;
import java.sql.Time;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Gives information about current node revisions state.
 */
public class RevisionsCommand implements Command {

    private static class RevisionsOptions extends Utils.NodeStoreOptions {

        public static final String CMD_INFO = "info";
        public static final String CMD_COLLECT = "collect";
        public static final String CMD_RESET = "reset";

        public final OptionSpec<?> once;
        public final OptionSpec<Integer> limit;
        public final OptionSpec<Long> olderThan;

        RevisionsOptions(String usage) {
            super(usage);
            once = parser.accepts("once", "only 1 iteration");
            limit = parser
                    .accepts("limit", "collect at most limit documents").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(-1);
            olderThan = parser
                    .accepts("olderThan", "collect only docs older than n seconds").withRequiredArg()
                    .ofType(Long.class).defaultsTo(Long.valueOf(3600 * 24));
        }

        public RevisionsOptions parse(String[] args) {
            super.parse(args);
            return this;
        }

        public String getSubCmd() {
            List<String> args = getOtherArgs();
            if (args.size() > 0) {
                return args.get(0);
            }
            return "info";
        }

        public boolean runOnce() {
            return options.has(once);
        }

        public int getLimit() {
            return limit.value(options);
        }

        public long getOlderThan() {
            return olderThan.value(options);
        }
    }

    @Override
    public void execute(String... args) throws Exception {
        Closer closer = Closer.create();
        RevisionsOptions options = new RevisionsOptions("revisions mongodb://host:port/database <subcmd> [options]\n"
            + "where subcmd is one of\n"
            + "  info     give information about the revisions state without performing any modifications\n"
            + "  collect  perform garbage collection.\n"
            + "  reset    clear all persisted metadata.\n"
            + "the following options are recognized:\n"
            + "  --limit n      collect at most n documents\n"
            + "  --olderThan n  collect only documents older than n seconds\n"
            + "  --once         run at maximum one iteration\n"
        ).parse(args);

        try {
            String subCmd = options.getSubCmd();
            NodeStore store = Utils.bootstrapNodeStore(options, closer);
            if (!(store instanceof DocumentNodeStore)) {
                System.err.println("revisions mode only available for DocumentNodeStore");
                System.exit(1);
            }
            DocumentNodeStore dns = (DocumentNodeStore) store;
            VersionGarbageCollector gc = dns.getVersionGarbageCollector();

            if (options.runOnce()) {
                gc.setMaxIterations(1);
            }
            if (options.getLimit() >= 0) {
                gc.setCollectLimit(options.getLimit());
            }

            if (RevisionsOptions.CMD_INFO.equals(subCmd)) {
                System.out.println("retrieving gc info");
                VersionGarbageCollector.VersionGCInfo info = gc.getInfo(options.getOlderThan(), TimeUnit.SECONDS);

                System.out.printf(Locale.US, "%21s  %s%n", "Last Successful Run:",
                        info.lastSuccess > 0? fmtTimestamp(info.lastSuccess) : "<unknown>");
                System.out.printf(Locale.US, "%21s  %s%n", "Oldest Revision:",
                        fmtTimestamp(info.oldestRevisionEstimate));
                System.out.printf(Locale.US, "%21s  %d%n", "Delete Candidates:",
                        info.revisionsCandidateCount);
                System.out.printf(Locale.US, "%21s  %d%n", "Collect Limit:",
                        info.collectLimit);
                System.out.printf(Locale.US, "%21s  %s%n", "Collect Interval:",
                        fmtDuration(info.recommendedCleanupInterval));
                System.out.printf(Locale.US, "%21s  %s%n", "Collect Before:",
                        fmtTimestamp(info.recommendedCleanupTimestamp));
                System.out.printf(Locale.US, "%21s  %d%n", "Iterations Estimate:",
                        info.estimatedIterations);
            }
            else if (RevisionsOptions.CMD_COLLECT.equals(subCmd)) {
                long started = System.currentTimeMillis();
                System.out.println("starting gc collect");
                VersionGarbageCollector.VersionGCStats stats = gc.gc(options.getOlderThan(), TimeUnit.SECONDS);
                long ended = System.currentTimeMillis();
                System.out.printf(Locale.US, "%21s  %s%n", "Started:", fmtTimestamp(started));
                System.out.printf(Locale.US, "%21s  %s%n", "Ended:", fmtTimestamp(ended));
                System.out.printf(Locale.US, "%21s  %s%n", "Duration:", fmtDuration(ended - started));
                System.out.printf(Locale.US, "%21s  %s%n", "Stats:", stats.toString());
            }
            else if (RevisionsOptions.CMD_RESET.equals(subCmd)) {
                System.out.println("resetting recommendations and statistics");
                gc.reset();
            }
            else {
                System.err.println("unknown revisions command: " + subCmd);
            }
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private String fmtTimestamp(long ts) {
        return org.apache.jackrabbit.oak.plugins.document.util.Utils.timestampToString(ts);
    }

    private String fmtDuration(long ts) {
        double d = (double)ts / TimeUnit.DAYS.toMillis(1);
        if (d > 1.0) {
            return String.format(Locale.US, "%.2f days", d);
        }
        d = (double)ts / TimeUnit.HOURS.toMillis(1);
        if (d > 1.0) {
            return String.format(Locale.US, "%.2f hours", d);
        }
        d = (double)ts / TimeUnit.MINUTES.toMillis(1);
        if (d > 1.0) {
            return String.format(Locale.US, "%.2f minutes", d);
        }
        d = (double)ts / TimeUnit.SECONDS.toMillis(1);
        return String.format(Locale.US, "%.4f seconds", d);
    }
}
