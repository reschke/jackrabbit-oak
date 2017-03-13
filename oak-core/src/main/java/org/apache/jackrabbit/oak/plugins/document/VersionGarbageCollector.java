/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.naming.LimitExceededException;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterators.partition;
import static com.google.common.util.concurrent.Atomics.newReference;
import static java.util.Collections.singletonMap;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.COMMIT_ROOT_ONLY;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_LEAF;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition.newEqualsCondition;

public class VersionGarbageCollector {
    //Kept less than MongoDocumentStore.IN_CLAUSE_BATCH_SIZE to avoid re-partitioning
    private static final int DELETE_BATCH_SIZE = 450;
    private static final int UPDATE_BATCH_SIZE = 450;
    private static final int PROGRESS_BATCH_SIZE = 10000;
    private static final Key KEY_MODIFIED = new Key(MODIFIED_IN_SECS, null);
    private final DocumentNodeStore nodeStore;
    private final DocumentStore ds;
    private final VersionGCSupport versionStore;
    private int overflowToDiskThreshold = 100000;
    private long collectLimit = overflowToDiskThreshold;
    private long precisionMs = TimeUnit.MINUTES.toMillis(1);
    private int maxIterations = 0;
    private long maxDurationMs = TimeUnit.HOURS.toMillis(0);
    private double modifyBatchDelayFactor  = 0;
    private final AtomicReference<GCJob> collector = newReference();

    private static final Logger log = LoggerFactory.getLogger(VersionGarbageCollector.class);

    /**
     * Split document types which can be safely garbage collected
     */
    private static final Set<NodeDocument.SplitDocType> GC_TYPES = EnumSet.of(
            DEFAULT_LEAF, COMMIT_ROOT_ONLY);

    /**
     * Document id stored in settings collection that keeps info about version gc
     */
    private static final String SETTINGS_COLLECTION_ID = "versionGC";

    /**
     * Property name to timestamp when last gc run happened
     */
    private static final String SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP = "lastOldestTimeStamp";

    /**
     * Property name to recommended time interval for next collection run
     */
    private static final String SETTINGS_COLLECTION_REC_INTERVAL_PROP = "recommendedIntervalMs";

    VersionGarbageCollector(DocumentNodeStore nodeStore,
                            VersionGCSupport gcSupport) {
        this.nodeStore = nodeStore;
        this.versionStore = gcSupport;
        this.ds = nodeStore.getDocumentStore();
    }

    public VersionGCStats gc(long maxRevisionAge, TimeUnit unit) throws IOException {
        long maxRevisionAgeInMillis = unit.toMillis(maxRevisionAge);
        TimeInterval maxRunTime = new TimeInterval(nodeStore.getClock().getTime(), Long.MAX_VALUE);
        if (maxDurationMs > 0) {
            maxRunTime = maxRunTime.startAndDuration(maxDurationMs);
        }
        GCJob job = new GCJob(maxRevisionAgeInMillis);
        if (collector.compareAndSet(null, job)) {
            VersionGCStats stats, overall = new VersionGCStats();
            overall.active.start();
            try {
                long averageDurationMs = 0;
                while (maxRunTime.contains(nodeStore.getClock().getTime() + averageDurationMs)) {
                    log.info("start {}. run (avg duration {} sec)",
                            overall.iterationCount + 1, averageDurationMs / 1000.0);
                    stats = job.run();

                    overall.addRun(stats);
                    if (maxIterations > 0 && overall.iterationCount >= maxIterations) {
                        break;
                    }
                    if (!overall.needRepeat) {
                        break;
                    }
                    averageDurationMs = ((averageDurationMs * (overall.iterationCount - 1))
                            + stats.active.elapsed(TimeUnit.MILLISECONDS)) / overall.iterationCount;
                }
                return overall;
            } finally {
                overall.active.stop();
                collector.set(null);
            }
        } else {
            throw new IOException("Revision garbage collection is already running");
        }
    }

    public void cancel() {
        GCJob job = collector.get();
        if (job != null) {
            job.cancel();
        }
    }

    public void reset() {
        Document versionGCDoc = ds.find(Collection.SETTINGS, SETTINGS_COLLECTION_ID, 0);
        if (versionGCDoc != null) {
            ds.remove(SETTINGS, versionGCDoc.getId());
        }
    }

    /**
     * Set the limit of number of resource id+_modified strings (not length) held in memory during
     * a collection run. Any more will be stored and sorted in a temporary file.
     * @param overflowToDiskThreshold limit after which to use file based storage for candidate ids
     */
    public void setOverflowToDiskThreshold(int overflowToDiskThreshold) {
        this.overflowToDiskThreshold = overflowToDiskThreshold;
    }

    /**
     * Sets the absolute limit on number of resource ids collected in one run. This does not count
     * nodes which can be deleted immediately. When this limit is exceeded, the run either fails or
     * is attempted with different parameters, depending on other settings. Note that if the inspected
     * time interval is equal or less than {@link #precisionMs}, the collection limit will be ignored.
     *
     * @param limit the absolute limit of resources collected in one run
     */
    public void setCollectLimit(long limit) {
        this.collectLimit = limit;
    }

    /**
     * Set the minimum duration that is used for time based searches. This should at minimum be the
     * precision available on modification dates of documents, but can be set larger to avoid querying
     * the database too often. Note however that {@link #collectLimit} will not take effect for runs
     * that query equal or shorter than precision duration.
     *
     * @param unit time unit used for duration
     * @param t    the number of units in the duration
     */
    public void setPrecisionMs(TimeUnit unit, long t) { this.precisionMs = unit.toMillis(t); }

    /**
     * Set the maximum duration in elapsed time that the garbage collection shall take. Setting this
     * to 0 means that there is no limit imposed. A positive duration will impose a soft limit, e.g.
     * the collection might take longer, but no next iteration will be attempted afterwards. See
     * {@link #setMaxIterations(int)} on how to control the behaviour.
     *
     * @param unit time unit used for duration
     * @param t    the number of units in the duration
     */
    public void setMaxDuration(TimeUnit unit, long t) { this.maxDurationMs = unit.toMillis(t); }

    /**
     * Set the maximum number of iterations that shall be attempted in a single run. A value
     * of 0 means that there is no limit. Since the garbage collector uses iterations to find
     * suitable time intervals and set sizes for cleanups, limiting the iterations is only
     * recommended for setups where the collector is called often.
     *
     * @param max the maximum number of iterations allowed
     */
    public void setMaxIterations(int max) { this.maxIterations = max; }

    /**
     * Set a delay factor between batched database modifications. This rate limits the writes
     * to the database by a garbage collector. 0, e.g. no delay, is the default. This is recommended
     * when garbage collection is done during a maintenance time when other system load is low.
     * <p>
     * For factory > 0, the actual delay is the duration of the last batch modification times
     * the factor. Example: 0.25 would result in a 25% delay, e.g. a batch modification running
     * 10 seconds would be followed by a sleep of 2.5 seconds.
     *
     * @param f the factor used to calculate batch modification delays
     */
    public void setModifyBatchDelayFactor(double f) {
        this.modifyBatchDelayFactor = f;
    }

    public VersionGCInfo getInfo(long maxRevisionAge, TimeUnit unit) throws IOException {
        long maxRevisionAgeInMillis = unit.toMillis(maxRevisionAge);
        long now = nodeStore.getClock().getTime();
        Recommendations rec = new Recommendations(maxRevisionAgeInMillis, precisionMs, collectLimit);
        return new VersionGCInfo(rec.lastOldestTimestamp, rec.scope.fromMs,
                rec.deleteCandidateCount, rec.maxCollect,
                rec.suggestedIntervalMs, rec.scope.toMs,
                (int)Math.ceil((now - rec.scope.toMs) / rec.suggestedIntervalMs));
    }

    public static class VersionGCInfo {
        public final long lastSuccess;
        public final long oldestRevisionEstimate;
        public final long revisionsCandidateCount;
        public final long collectLimit;
        public final long recommendedCleanupInterval;
        public final long recommendedCleanupTimestamp;
        public final int estimatedIterations;

        VersionGCInfo(long lastSuccess,
                      long oldestRevisionEstimate,
                      long revisionsCandidateCount,
                      long collectLimit,
                      long recommendedCleanupInterval,
                      long recommendedCleanupTimestamp,
                      int estimatedIterations) {
            this.lastSuccess = lastSuccess;
            this.oldestRevisionEstimate = oldestRevisionEstimate;
            this.revisionsCandidateCount = revisionsCandidateCount;
            this.collectLimit = collectLimit;
            this.recommendedCleanupInterval = recommendedCleanupInterval;
            this.recommendedCleanupTimestamp = recommendedCleanupTimestamp;
            this.estimatedIterations = estimatedIterations;
        }
    }

    public static class VersionGCStats {
        boolean ignoredGCDueToCheckPoint;
        boolean canceled;
        boolean limitExceeded;
        boolean needRepeat;
        int iterationCount;
        int deletedDocGCCount;
        int deletedLeafDocGCCount;
        int splitDocGCCount;
        int intermediateSplitDocGCCount;
        int updateResurrectedGCCount;
        final Stopwatch active = Stopwatch.createUnstarted();
        final Stopwatch collectDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch checkDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch collectAndDeleteSplitDocs = Stopwatch.createUnstarted();
        final Stopwatch sortDocIds = Stopwatch.createUnstarted();
        final Stopwatch updateResurrectedDocuments = Stopwatch.createUnstarted();

        @Override
        public String toString() {
            return "VersionGCStats{" +
                    "ignoredGCDueToCheckPoint=" + ignoredGCDueToCheckPoint +
                    ", canceled=" + canceled +
                    ", deletedDocGCCount=" + deletedDocGCCount + " (of which leaf: " + deletedLeafDocGCCount + ")" +
                    ", updateResurrectedGCCount=" + updateResurrectedGCCount +
                    ", splitDocGCCount=" + splitDocGCCount +
                    ", intermediateSplitDocGCCount=" + intermediateSplitDocGCCount +
                    ", iterationCount=" + iterationCount +
                    ", timeActive=" + active +
                    ", timeToCollectDeletedDocs=" + collectDeletedDocs +
                    ", timeToCheckDeletedDocs=" + checkDeletedDocs +
                    ", timeToSortDocIds=" + sortDocIds +
                    ", timeTakenToUpdateResurrectedDocs=" + updateResurrectedDocuments +
                    ", timeTakenToDeleteDeletedDocs=" + deleteDeletedDocs +
                    ", timeTakenToCollectAndDeleteSplitDocs=" + collectAndDeleteSplitDocs +
                    '}';
        }

        void addRun(VersionGCStats run) {
            ++iterationCount;
            this.ignoredGCDueToCheckPoint = run.ignoredGCDueToCheckPoint;
            this.canceled = run.canceled;
            this.limitExceeded = run.limitExceeded;
            this.needRepeat = run.needRepeat;
            this.deletedDocGCCount += run.deletedDocGCCount;
            this.deletedLeafDocGCCount += run.deletedLeafDocGCCount;
            this.splitDocGCCount += run.splitDocGCCount;
            this.intermediateSplitDocGCCount += run.intermediateSplitDocGCCount;
            this.updateResurrectedGCCount += run.updateResurrectedGCCount;
            /* TODO: add stopwatch values */
        }
    }

    private enum GCPhase {
        NONE,
        COLLECTING,
        CHECKING,
        DELETING,
        SORTING,
        SPLITS_CLEANUP,
        UPDATING
    }

    /**
     * Keeps track of timers when switching GC phases.
     * <p>
     * Could be merged with VersionGCStats, however this way the public class is kept unchanged.
     */
    private static class GCPhases {

        final VersionGCStats stats;
        final Stopwatch elapsed;
        private final List<GCPhase> phases = Lists.newArrayList();
        private final Map<GCPhase, Stopwatch> watches = Maps.newHashMap();
        private final AtomicBoolean canceled;

        GCPhases(AtomicBoolean canceled, VersionGCStats stats) {
            this.stats = stats;
            this.elapsed = Stopwatch.createStarted();
            this.watches.put(GCPhase.NONE, Stopwatch.createStarted());
            this.watches.put(GCPhase.COLLECTING, stats.collectDeletedDocs);
            this.watches.put(GCPhase.CHECKING, stats.checkDeletedDocs);
            this.watches.put(GCPhase.DELETING, stats.deleteDeletedDocs);
            this.watches.put(GCPhase.SORTING, stats.sortDocIds);
            this.watches.put(GCPhase.SPLITS_CLEANUP, stats.collectAndDeleteSplitDocs);
            this.watches.put(GCPhase.UPDATING, stats.updateResurrectedDocuments);
            this.canceled = canceled;
        }

        /**
         * Attempts to start a GC phase and tracks the time spent in this phase
         * until {@link #stop(GCPhase)} is called.
         *
         * @param started the GC phase.
         * @return {@code true} if the phase was started or {@code false} if the
         * revision GC was canceled and the phase should not start.
         */
        public boolean start(GCPhase started) {
            if (canceled.get()) {
                return false;
            }
            suspend(currentWatch());
            this.phases.add(started);
            resume(currentWatch());
            return true;
        }

        public void stop(GCPhase phase) {
            if (!phases.isEmpty() && phase == phases.get(phases.size() - 1)) {
                suspend(currentWatch());
                phases.remove(phases.size() - 1);
                resume(currentWatch());
            }
        }

        public void close() {
            while (!phases.isEmpty()) {
                suspend(currentWatch());
                phases.remove(phases.size() - 1);
            }
            this.elapsed.stop();
        }

        private GCPhase current() {
            return phases.isEmpty() ? GCPhase.NONE : phases.get(phases.size() - 1);
        }

        private Stopwatch currentWatch() {
            return watches.get(current());
        }

        private void resume(Stopwatch w) {
            if (!w.isRunning()) {
                w.start();
            }
        }

        private void suspend(Stopwatch w) {
            if (w.isRunning()) {
                w.stop();
            }
        }
    }

    private class GCJob {

        private final long maxRevisionAgeMillis;
        private AtomicBoolean cancel = new AtomicBoolean();

        GCJob(long maxRevisionAgeMillis) {
            this.maxRevisionAgeMillis = maxRevisionAgeMillis;
        }

        VersionGCStats run() throws IOException {
            return gc(maxRevisionAgeMillis);
        }

        void cancel() {
            log.info("Canceling revision garbage collection.");
            cancel.set(true);
        }

        private VersionGCStats gc(long maxRevisionAgeInMillis) throws IOException {
            VersionGCStats stats = new VersionGCStats();
            stats.active.start();
            Recommendations rec = new Recommendations(maxRevisionAgeInMillis, precisionMs, collectLimit);
            GCPhases phases = new GCPhases(cancel, stats);
            try {
                if (rec.ignoreDueToCheckPoint) {
                    phases.stats.ignoredGCDueToCheckPoint = true;
                    cancel.set(true);
                } else {
                    final RevisionVector headRevision = nodeStore.getHeadRevision();
                    log.info("Looking at revisions in {}", rec.scope);

                    collectDeletedDocuments(phases, headRevision, rec);
                    collectSplitDocuments(phases, rec);
                }
            } catch (LimitExceededException ex) {
                stats.limitExceeded = true;
            } finally {
                phases.close();
                stats.canceled = cancel.get();
            }

            rec.evaluate(stats);
            log.info("Revision garbage collection finished in {}. {}", phases.elapsed, stats);
            stats.active.stop();
            return stats;
        }

        private void collectSplitDocuments(GCPhases phases, Recommendations rec) {
            if (phases.start(GCPhase.SPLITS_CLEANUP)) {
                versionStore.deleteSplitDocuments(GC_TYPES, rec.scope.toMs, phases.stats);
                phases.stop(GCPhase.SPLITS_CLEANUP);
            }
        }

        private void collectDeletedDocuments(GCPhases phases,
                                             RevisionVector headRevision,
                                             Recommendations rec)
                throws IOException, LimitExceededException {
            int docsTraversed = 0;
            DeletedDocsGC gc = new DeletedDocsGC(headRevision, cancel);
            try {
                if (phases.start(GCPhase.COLLECTING)) {
                    Iterable<NodeDocument> itr = versionStore.getPossiblyDeletedDocs(rec.scope.fromMs, rec.scope.toMs);
                    try {
                        for (NodeDocument doc : itr) {
                            // continue with GC?
                            if (cancel.get()) {
                                break;
                            }
                            // Check if node is actually deleted at current revision
                            // As node is not modified since oldestRevTimeStamp then
                            // this node has not be revived again in past maxRevisionAge
                            // So deleting it is safe
                            docsTraversed++;
                            if (docsTraversed % PROGRESS_BATCH_SIZE == 0) {
                                log.info("Iterated through {} documents so far. {} found to be deleted",
                                        docsTraversed, gc.getNumDocuments());
                            }
                            if (phases.start(GCPhase.CHECKING)) {
                                gc.possiblyDeleted(doc);
                                phases.stop(GCPhase.CHECKING);
                            }
                            if (rec.maxCollect > 0 && gc.docIdsToDelete.getSize() > rec.maxCollect) {
                                throw new LimitExceededException();
                            }
                            if (gc.hasLeafBatch()) {
                                if (phases.start(GCPhase.DELETING)) {
                                    gc.removeLeafDocuments(phases.stats);
                                    phases.stop(GCPhase.DELETING);
                                }
                            }
                            if (gc.hasRescurrectUpdateBatch()) {
                                if (phases.start(GCPhase.UPDATING)) {
                                    gc.updateResurrectedDocuments(phases.stats);
                                    phases.stop(GCPhase.UPDATING);
                                }
                            }
                        }
                    } finally {
                        Utils.closeIfCloseable(itr);
                    }
                    phases.stop(GCPhase.COLLECTING);
                }

                if (gc.getNumDocuments() != 0) {
                    if (phases.start(GCPhase.DELETING)) {
                        gc.removeLeafDocuments(phases.stats);
                        phases.stop(GCPhase.DELETING);
                    }

                    if (phases.start(GCPhase.SORTING)) {
                        gc.ensureSorted();
                        phases.stop(GCPhase.SORTING);
                    }

                    if (phases.start(GCPhase.DELETING)) {
                        gc.removeDocuments(phases.stats);
                        phases.stop(GCPhase.DELETING);
                    }
                }

                if (phases.start(GCPhase.UPDATING)) {
                    gc.updateResurrectedDocuments(phases.stats);
                    phases.stop(GCPhase.UPDATING);
                }
            } finally {
                gc.close();
            }
        }
    }

    /**
     * A helper class to remove document for deleted nodes.
     */
    private class DeletedDocsGC implements Closeable {

        private final RevisionVector headRevision;
        private final AtomicBoolean cancel;
        private final List<String> leafDocIdsToDelete = Lists.newArrayList();
        private final List<String> resurrectedIds = Lists.newArrayList();
        private final StringSort docIdsToDelete = newStringSort();
        private final StringSort prevDocIdsToDelete = newStringSort();
        private final Set<String> exclude = Sets.newHashSet();
        private boolean sorted = false;
        private final Stopwatch timer;

        public DeletedDocsGC(@Nonnull RevisionVector headRevision,
                             @Nonnull AtomicBoolean cancel) {
            this.headRevision = checkNotNull(headRevision);
            this.cancel = checkNotNull(cancel);
            this.timer = Stopwatch.createUnstarted();
        }

        /**
         * @return the number of documents gathers so far that have been
         * identified as garbage via {@link #possiblyDeleted(NodeDocument)}.
         * This number does not include the previous documents.
         */
        long getNumDocuments() {
            return docIdsToDelete.getSize() + leafDocIdsToDelete.size();
        }

        /**
         * Informs the GC that the given document is possibly deleted. The
         * implementation will check if the node still exists at the head
         * revision passed to the constructor to this GC. The implementation
         * will keep track of documents representing deleted nodes and remove
         * them together with associated previous document
         *
         * @param doc the candidate document.
         * @return true iff document is scheduled for deletion
         */
        boolean possiblyDeleted(NodeDocument doc)
                throws IOException {
            // construct an id that also contains
            // the _modified time of the document
            String id = doc.getId() + "/" + doc.getModified();
            // check if id is valid
            try {
                Utils.getDepthFromId(id);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid GC id {} for document {}", id, doc);
                return false;
            }
            if (doc.getNodeAtRevision(nodeStore, headRevision, null) == null) {
                // Collect id of all previous docs also
                Iterator<String> previousDocs = previousDocIdsFor(doc);
                if (!doc.hasChildren() && !previousDocs.hasNext()) {
                    addLeafDocument(id);
                } else {
                    addDocument(id);
                    addPreviousDocuments(previousDocs);
                }
                return true;
            } else {
                addNonDeletedDocument(id);
            }
            return false;
        }

        /**
         * Removes the documents that have been identified as garbage. This
         * also includes previous documents. This method will only remove
         * documents that have not been modified since they were passed to
         * {@link #possiblyDeleted(NodeDocument)}.
         *
         * @param stats to track the number of removed documents.
         */
        void removeDocuments(VersionGCStats stats) throws IOException {
            removeLeafDocuments(stats);
            stats.deletedDocGCCount += removeDeletedDocuments(
                    getDocIdsToDelete(), getDocIdsToDeleteSize(), "(other)");
            // FIXME: this is incorrect because that method also removes intermediate docs
            stats.splitDocGCCount += removeDeletedPreviousDocuments();
        }

        boolean hasLeafBatch() {
            return leafDocIdsToDelete.size() >= DELETE_BATCH_SIZE;
        }

        boolean hasRescurrectUpdateBatch() {
            return resurrectedIds.size() >= UPDATE_BATCH_SIZE;
        }

        void removeLeafDocuments(VersionGCStats stats) throws IOException {
            int removeCount = removeDeletedDocuments(
                    getLeafDocIdsToDelete(), getLeafDocIdsToDeleteSize(), "(leaf)");
            leafDocIdsToDelete.clear();
            stats.deletedLeafDocGCCount += removeCount;
            stats.deletedDocGCCount += removeCount;
        }

        void updateResurrectedDocuments(VersionGCStats stats) throws IOException {
            int updateCount = resetDeletedOnce(resurrectedIds);
            resurrectedIds.clear();
            stats.updateResurrectedGCCount += updateCount;
        }

        public void close() {
            try {
                docIdsToDelete.close();
            } catch (IOException e) {
                log.warn("Failed to close docIdsToDelete", e);
            }
            try {
                prevDocIdsToDelete.close();
            } catch (IOException e) {
                log.warn("Failed to close prevDocIdsToDelete", e);
            }
        }

        //------------------------------< internal >----------------------------

        private void delayOnModifications(long durationMs) {
            long delayMs = (long)Math.round(maxDurationMs * modifyBatchDelayFactor);
            if (!cancel.get() && modifyBatchDelayFactor > 0) {
                try {
                    Thread.sleep(delayMs);
                }
                catch (InterruptedException ex) {
                    /* ignore */
                }
            }
        }

        private Iterator<String> previousDocIdsFor(NodeDocument doc) {
            Map<Revision, Range> prevRanges = doc.getPreviousRanges(true);
            if (prevRanges.isEmpty()) {
                return Iterators.emptyIterator();
            } else if (all(prevRanges.values(), FIRST_LEVEL)) {
                // all previous document ids can be constructed from the
                // previous ranges map. this works for first level previous
                // documents only.
                final String path = doc.getPath();
                return Iterators.transform(prevRanges.entrySet().iterator(),
                        new Function<Map.Entry<Revision, Range>, String>() {
                            @Override
                            public String apply(Map.Entry<Revision, Range> input) {
                                int h = input.getValue().getHeight();
                                return Utils.getPreviousIdFor(path, input.getKey(), h);
                            }
                        });
            } else {
                // need to fetch the previous documents to get their ids
                return Iterators.transform(doc.getAllPreviousDocs(),
                        new Function<NodeDocument, String>() {
                            @Override
                            public String apply(NodeDocument input) {
                                return input.getId();
                            }
                        });
            }
        }

        private void addDocument(String id) throws IOException {
            docIdsToDelete.add(id);
        }

        private void addLeafDocument(String id) throws IOException {
            leafDocIdsToDelete.add(id);
        }

        private void addNonDeletedDocument(String id) throws IOException {
            resurrectedIds.add(id);
        }

        private long getNumPreviousDocuments() {
            return prevDocIdsToDelete.getSize() - exclude.size();
        }

        private void addPreviousDocuments(Iterator<String> ids) throws IOException {
            while (ids.hasNext()) {
                prevDocIdsToDelete.add(ids.next());
            }
        }

        private Iterator<String> getDocIdsToDelete() throws IOException {
            ensureSorted();
            return docIdsToDelete.getIds();
        }

        private long getDocIdsToDeleteSize() {
            return docIdsToDelete.getSize();
        }

        private Iterator<String> getLeafDocIdsToDelete() throws IOException {
            return leafDocIdsToDelete.iterator();
        }

        private long getLeafDocIdsToDeleteSize() {
            return leafDocIdsToDelete.size();
        }

        private void concurrentModification(NodeDocument doc) {
            Iterator<NodeDocument> it = doc.getAllPreviousDocs();
            while (it.hasNext()) {
                exclude.add(it.next().getId());
            }
        }

        private Iterator<String> getPrevDocIdsToDelete() throws IOException {
            ensureSorted();
            return Iterators.filter(prevDocIdsToDelete.getIds(),
                    new Predicate<String>() {
                        @Override
                        public boolean apply(String input) {
                            return !exclude.contains(input);
                        }
                    });
        }

        private int removeDeletedDocuments(Iterator<String> docIdsToDelete,
                                           long numDocuments,
                                           String label) throws IOException {
            log.info("Proceeding to delete [{}] documents [{}]", numDocuments, label);

            Iterator<List<String>> idListItr = partition(docIdsToDelete, DELETE_BATCH_SIZE);
            int deletedCount = 0;
            int lastLoggedCount = 0;
            int recreatedCount = 0;
            while (idListItr.hasNext() && !cancel.get()) {
                Map<String, Map<Key, Condition>> deletionBatch = Maps.newLinkedHashMap();
                for (String s : idListItr.next()) {
                    Map.Entry<String, Long> parsed;
                    try {
                        parsed = parseEntry(s);
                    } catch (IllegalArgumentException e) {
                        log.warn("Invalid _modified suffix for {}", s);
                        continue;
                    }
                    deletionBatch.put(parsed.getKey(), singletonMap(KEY_MODIFIED, newEqualsCondition(parsed.getValue())));
                }

                if (log.isTraceEnabled()) {
                    StringBuilder sb = new StringBuilder("Performing batch deletion of documents with following ids. \n");
                    Joiner.on(LINE_SEPARATOR.value()).appendTo(sb, deletionBatch.keySet());
                    log.trace(sb.toString());
                }

                timer.reset().start();
                try {
                    int nRemoved = ds.remove(NODES, deletionBatch);

                    if (nRemoved < deletionBatch.size()) {
                        // some nodes were re-created while GC was running
                        // find the document that still exist
                        for (String id : deletionBatch.keySet()) {
                            NodeDocument d = ds.find(NODES, id);
                            if (d != null) {
                                concurrentModification(d);
                            }
                        }
                        recreatedCount += (deletionBatch.size() - nRemoved);
                    }

                    deletedCount += nRemoved;
                    log.debug("Deleted [{}] documents so far", deletedCount);

                    if (deletedCount + recreatedCount - lastLoggedCount >= PROGRESS_BATCH_SIZE) {
                        lastLoggedCount = deletedCount + recreatedCount;
                        double progress = lastLoggedCount * 1.0 / getNumDocuments() * 100;
                        String msg = String.format("Deleted %d (%1.2f%%) documents so far", deletedCount, progress);
                        log.info(msg);
                    }
                } finally {
                    delayOnModifications(timer.stop().elapsed(TimeUnit.MILLISECONDS));
                }
            }
            return deletedCount;
        }

        private int resetDeletedOnce(List<String> resurrectedDocuments) throws IOException {
            log.info("Proceeding to reset [{}] _deletedOnce flags", resurrectedDocuments.size());

            int updateCount = 0;
            timer.reset().start();
            try {
                for (String s : resurrectedDocuments) {
                    if (!cancel.get()) {
                        try {
                            Map.Entry<String, Long> parsed = parseEntry(s);
                            UpdateOp up = new UpdateOp(parsed.getKey(), false);
                            up.equals(MODIFIED_IN_SECS, parsed.getValue());
                            up.remove(NodeDocument.DELETED_ONCE);
                            NodeDocument r = ds.findAndUpdate(Collection.NODES, up);
                            if (r != null) {
                                updateCount += 1;
                            }
                        } catch (IllegalArgumentException ex) {
                            log.warn("Invalid _modified suffix for {}", s);
                        } catch (DocumentStoreException ex) {
                            log.warn("updating {}: {}", s, ex.getMessage());
                        }
                    }
                }
            }
            finally {
                delayOnModifications(timer.stop().elapsed(TimeUnit.MILLISECONDS));
            }
            return updateCount;
        }

        private int removeDeletedPreviousDocuments() throws IOException {
            log.info("Proceeding to delete [{}] previous documents", getNumPreviousDocuments());

            int deletedCount = 0;
            int lastLoggedCount = 0;
            Iterator<List<String>> idListItr =
                    partition(getPrevDocIdsToDelete(), DELETE_BATCH_SIZE);
            while (idListItr.hasNext() && !cancel.get()) {
                List<String> deletionBatch = idListItr.next();
                deletedCount += deletionBatch.size();

                if (log.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder("Performing batch deletion of previous documents with following ids. \n");
                    Joiner.on(LINE_SEPARATOR.value()).appendTo(sb, deletionBatch);
                    log.debug(sb.toString());
                }

                ds.remove(NODES, deletionBatch);

                log.debug("Deleted [{}] previous documents so far", deletedCount);

                if (deletedCount - lastLoggedCount >= PROGRESS_BATCH_SIZE) {
                    lastLoggedCount = deletedCount;
                    double progress = deletedCount * 1.0 / (prevDocIdsToDelete.getSize() - exclude.size()) * 100;
                    String msg = String.format("Deleted %d (%1.2f%%) previous documents so far", deletedCount, progress);
                    log.info(msg);
                }
            }
            return deletedCount;
        }

        private void ensureSorted() throws IOException {
            if (!sorted) {
                docIdsToDelete.sort();
                prevDocIdsToDelete.sort();
                sorted = true;
            }
        }

        /**
         * Parses an id/modified entry and returns the two components as a
         * Map.Entry.
         *
         * @param entry the id/modified String.
         * @return the parsed components.
         * @throws IllegalArgumentException if the entry is malformed.
         */
        private Map.Entry<String, Long> parseEntry(String entry) throws IllegalArgumentException {
            int idx = entry.lastIndexOf('/');
            if (idx == -1) {
                throw new IllegalArgumentException(entry);
            }
            String id = entry.substring(0, idx);
            long modified;
            try {
                modified = Long.parseLong(entry.substring(idx + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(entry);
            }
            return Maps.immutableEntry(id, modified);
        }
    }

    @Nonnull
    private StringSort newStringSort() {
        return new StringSort(overflowToDiskThreshold,
                NodeDocumentIdComparator.INSTANCE);
    }

    private static final Predicate<Range> FIRST_LEVEL = new Predicate<Range>() {
        @Override
        public boolean apply(@Nullable Range input) {
            return input != null && input.height == 0;
        }
    };

    private class Recommendations {
        final boolean ignoreDueToCheckPoint;
        final TimeInterval scope;
        final long maxCollect;
        final long deleteCandidateCount;
        final long lastOldestTimestamp;

        private final long precisionMs;
        private final long suggestedIntervalMs;
        private final boolean scopeIsComplete;

        /**
         * Gives a recommendation about parameters for the next revision garbage collection run.
         * <p>
         * With the given maximum age of revisions to keep (earliest time in the past to collect),
         * the desired precision in which times shall be sliced and the given limit on the number
         * of collected documents in one run, calculate <ol>
         *     <li>if gc shall run at all (ignoreDueToCheckPoint)</li>
         *     <li>in which time interval documents shall be collected (scope)</li>
         *     <li>if collection should fail if it reaches maxCollect documents, maxCollect will specify
         *     the limit or be 0 if no limit shall be enforced.</li>
         * </ol>
         * After a run, recommendations evaluate the result of the gc to update its persisted recommendations
         * for future runs.
         * <p>
         * In the settings collection, recommendations keeps "revisionsOlderThan" from the last successful run.
         * It also updates the time interval recommended for the next run.
         *
         * @param maxRevisionAgeMs the minimum age for revisions to be collected
         * @param precisionMs the minimum time interval to be used
         * @param collectLimit the desired maximum amount of documents to be collected
         */
        Recommendations(long maxRevisionAgeMs, long precisionMs, long collectLimit) {
            TimeInterval keep = new TimeInterval(nodeStore.getClock().getTime() - maxRevisionAgeMs, Long.MAX_VALUE);
            boolean ignoreDueToCheckPoint = false;
            long deletedOnceCount = 0;
            long suggestedIntervalMs;
            long oldestPossible;

            lastOldestTimestamp = getLongSetting(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP);
            if (lastOldestTimestamp == 0) {
                log.debug("No lastOldestTimestamp found, querying for the oldest deletedOnce candidate");
                oldestPossible = versionStore.getOldestDeletedOnceTimestamp(nodeStore.getClock(), precisionMs) - 1;
                log.debug("lastOldestTimestamp found: {}", Utils.timestampToString(oldestPossible));
            } else {
                oldestPossible = lastOldestTimestamp - 1;
            }

            TimeInterval scope = new TimeInterval(oldestPossible, Long.MAX_VALUE);
            scope = scope.notLaterThan(keep.fromMs);

            suggestedIntervalMs = getLongSetting(SETTINGS_COLLECTION_REC_INTERVAL_PROP);
            if (suggestedIntervalMs > 0) {
                suggestedIntervalMs = Math.max(suggestedIntervalMs, precisionMs);
                if (suggestedIntervalMs < scope.getDurationMs()) {
                    scope = scope.startAndDuration(suggestedIntervalMs);
                    log.debug("previous runs recommend a {} sec duration, scope now {}",
                            TimeUnit.MILLISECONDS.toSeconds(suggestedIntervalMs), scope);
                }
            } else {
                /* Need to guess. Count the overall number of _deletedOnce documents. If those
                 * are more than we want to collect in a single run, reduce the time scope so
                 * that we likely see a fitting fraction of those documents.
                 */
                try {
                    long preferredLimit = Math.min(collectLimit, (long)Math.ceil(overflowToDiskThreshold * 0.95));
                    deletedOnceCount = versionStore.getDeletedOnceCount();
                    if (deletedOnceCount > preferredLimit) {
                        double chunks = ((double) deletedOnceCount) / preferredLimit;
                        suggestedIntervalMs = (long) Math.floor((scope.getDurationMs() + maxRevisionAgeMs) / chunks);
                        if (suggestedIntervalMs < scope.getDurationMs()) {
                            scope = scope.startAndDuration(suggestedIntervalMs);
                            log.debug("deletedOnce candidates: {} found, {} preferred, scope now {}",
                                    deletedOnceCount, preferredLimit, scope);
                        }
                    }
                } catch (UnsupportedOperationException ex) {
                    log.debug("check on upper bounds of delete candidates not supported, skipped");
                }
            }

            //Check for any registered checkpoint which prevent the GC from running
            Revision checkpoint = nodeStore.getCheckpoints().getOldestRevisionToKeep();
            if (checkpoint != null && scope.endsAfter(checkpoint.getTimestamp())) {
                TimeInterval minimalScope = scope.startAndDuration(precisionMs);
                if (minimalScope.endsAfter(checkpoint.getTimestamp())) {
                    log.warn("Ignoring RGC run because a valid checkpoint [{}] exists inside minimal scope {}.",
                            checkpoint.toReadableString(), minimalScope);
                    ignoreDueToCheckPoint = true;
                } else {
                    scope = scope.notLaterThan(checkpoint.getTimestamp() - 1);
                    log.debug("checkpoint at [{}] found, scope now {}",
                            Utils.timestampToString(checkpoint.getTimestamp()), scope);
                }
            }

            if (scope.getDurationMs() <= precisionMs) {
                // If we have narrowed the collect time interval down as much as we can, no
                // longer enforce a limit. We need to get through this.
                collectLimit = 0;
                log.debug("time interval <= precision ({} ms), disabling collection limits", precisionMs);
            }

            this.precisionMs = precisionMs;
            this.ignoreDueToCheckPoint = ignoreDueToCheckPoint;
            this.scope = scope;
            this.scopeIsComplete = scope.toMs >= keep.fromMs;
            this.maxCollect = collectLimit;
            this.suggestedIntervalMs = suggestedIntervalMs;
            this.deleteCandidateCount = deletedOnceCount;
        }

        /**
         * Evaluate the results of the last run. Update recommendations for future runs.
         * Will set {@link VersionGCStats#needRepeat} if collection needs to run another
         * iteration for collecting documents up to "now".
         *
         * @param stats the statistics from the last run
         */
        public void evaluate(VersionGCStats stats) {
            if (stats.limitExceeded) {
                // if the limit was exceeded, slash the recommended interval in half.
                long nextDuration = Math.max(precisionMs, scope.getDurationMs() / 2);
                log.info("Limit {} documents exceeded, reducing next collection interval to {} seconds",
                        this.maxCollect, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
                setLongSetting(SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
                stats.needRepeat = true;
            } else if (!stats.canceled && !stats.ignoredGCDueToCheckPoint) {
                // success, we would not expect to encounter revisions older than this in the future
                setLongSetting(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, scope.toMs);

                if (maxCollect <= 0) {
                    log.debug("successful run without effective limit, keeping recommendations");
                } else if (scope.getDurationMs() == suggestedIntervalMs) {
                    int count = stats.deletedDocGCCount - stats.deletedLeafDocGCCount;
                    double used = count / (double) maxCollect;
                    if (used < 0.66) {
                        long nextDuration = (long) Math.ceil(suggestedIntervalMs * 1.5);
                        log.debug("successful run using {}% of limit, raising recommended interval to {} seconds",
                                Math.round(used*1000)/10.0, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
                        setLongSetting(SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
                    }
                } else {
                    log.debug("successful run not following recommendations, keeping them");
                }
                stats.needRepeat = !scopeIsComplete;
            }
        }

        private long getLongSetting(String propName) {
            Document versionGCDoc = ds.find(Collection.SETTINGS, SETTINGS_COLLECTION_ID, 0);
            if (versionGCDoc != null) {
                Long l = (Long) versionGCDoc.get(propName);
                if (l != null) {
                    return l;
                }
            }
            return 0;
        }

        private void setLongSetting(String propName, long val) {
            UpdateOp updateOp = new UpdateOp(SETTINGS_COLLECTION_ID,
                    (ds.find(Collection.SETTINGS, SETTINGS_COLLECTION_ID) == null));
            updateOp.set(propName, val);
            ds.createOrUpdate(Collection.SETTINGS, updateOp);
        }
    }

    public static class TimeInterval {
        public final long fromMs;
        public final long toMs;

        public TimeInterval(long fromMs, long toMs) {
            checkArgument(fromMs <= toMs);
            this.fromMs = fromMs;
            this.toMs = toMs;
        }

        public TimeInterval notLaterThan(long timestampMs) {
            if (timestampMs < toMs) {
                return new TimeInterval((timestampMs < fromMs)? timestampMs : fromMs, timestampMs);
            }
            return this;
        }

        public TimeInterval notEarlierThan(long timestampMs) {
            if (fromMs < timestampMs) {
                return new TimeInterval(timestampMs, (timestampMs < toMs)? timestampMs : toMs);
            }
            return this;
        }

        public TimeInterval startAndDuration(long durationMs) {
            return new TimeInterval(fromMs, fromMs + durationMs);
        }

        public long getDurationMs() {
            return toMs - fromMs;
        }

        public boolean contains(long timestampMs) {
            return fromMs <= timestampMs && timestampMs <= toMs;
        }

        public boolean endsAfter(long timestampMs) {
            return toMs > timestampMs;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TimeInterval) {
                return fromMs == ((TimeInterval) o).fromMs && toMs == ((TimeInterval) o).toMs;
            }
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return (int)(fromMs^(fromMs>>>32)^toMs^(toMs>>>32));
        }

        @Override
        public String toString() {
            return "[" + Utils.timestampToString(fromMs) + ", " + Utils.timestampToString(toMs) + "]";
        }
    }
}
