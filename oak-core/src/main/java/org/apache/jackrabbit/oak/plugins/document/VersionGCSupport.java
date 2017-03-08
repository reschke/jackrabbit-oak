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

import static com.google.common.collect.Iterables.filter;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getSelectedDocuments;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.lang.ref.Reference;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

import java.util.concurrent.TimeUnit;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionGCSupport {

    private static final Logger Log = LoggerFactory.getLogger(VersionGCSupport.class);

    private final DocumentStore store;

    public VersionGCSupport(DocumentStore store) {
        this.store = store;
    }

    public Iterable<NodeDocument> getPossiblyDeletedDocs(final long lastModifiedTime) {
        return filter(getSelectedDocuments(store, NodeDocument.DELETED_ONCE, 1), new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                return input.wasDeletedOnce() && !input.hasBeenModifiedSince(lastModifiedTime);
            }
        });
    }

    public void deleteSplitDocuments(Set<SplitDocType> gcTypes,
                                     long oldestRevTimeStamp,
                                     VersionGCStats stats) {
        stats.splitDocGCCount += createCleanUp(gcTypes, oldestRevTimeStamp, stats)
                .disconnect()
                .deleteSplitDocuments();
    }

    protected SplitDocumentCleanUp createCleanUp(Set<SplitDocType> gcTypes,
                                                 long oldestRevTimeStamp,
                                                 VersionGCStats stats) {
        return new SplitDocumentCleanUp(store, stats,
                identifyGarbage(gcTypes, oldestRevTimeStamp));
    }

    protected Iterable<NodeDocument> identifyGarbage(final Set<SplitDocType> gcTypes,
                                                     final long oldestRevTimeStamp) {
        return filter(getAllDocuments(store), new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument doc) {
                return gcTypes.contains(doc.getSplitDocType())
                        && doc.hasAllRevisionLessThan(oldestRevTimeStamp);
            }
        });
    }

    /**
     * Retrieve the time of the oldest document marked as 'deletedOnce'.
     *
     * @param precisionMs the exact time may vary by given precision
     * @return the timestamp of the oldest document marked with 'deletecOnce',
     *          module given prevision. If no such document exists, returns the
     *          max time inspected (close to current time).
     */
    public long getOldestDeletedOnceTimestamp(Clock clock, long precisionMs) {
        long ts = 0;
        long now = clock.getTime();
        long duration =  (now - ts) / 2;
        Iterable<NodeDocument> docs;

        while (duration > precisionMs) {
            // check for delete candidates in [ ts, ts + duration]
            Log.debug("find oldest _deletedOnce, check < {}", Utils.timestampToString(ts + duration));
            docs = getPossiblyDeletedDocs(ts + duration);
            if (docs.iterator().hasNext()) {
                // look if there are still nodes to be found in the lower half
                duration /= 2;
            }
            else {
                // so, there are no delete candidates older than "ts + duration"
                ts = ts + duration;
                duration /= 2;
            }
            if (docs instanceof Closeable) {
                IOUtils.closeQuietly((Closeable)docs);
            }
        }
        Log.debug("find oldest _deletedOnce to be {}", Utils.timestampToString(ts));
        return ts;
    }

    public long getDeletedOnceCount() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("getDeletedOnceCount()");
    }
}
