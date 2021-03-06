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
package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.Segment.RecordConsumer;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The storage implementation for tar files.
 */
public abstract class AbstractFileStore implements SegmentStore, Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractFileStore.class);

    private static final String MANIFEST_FILE_NAME = "manifest";

    /**
     * This value can be used as an invalid store version, since the store
     * version is defined to be strictly greater than zero.
     */
    private static final int INVALID_STORE_VERSION = 0;

    /**
     * The store version is an always incrementing number, strictly greater than
     * zero, that is changed every time there is a backwards incompatible
     * modification to the format of the segment store.
     */
    static final int CURRENT_STORE_VERSION = 1;

    private static final Pattern FILE_NAME_PATTERN =
            Pattern.compile("(data)((0|[1-9][0-9]*)[0-9]{4})([a-z])?.tar");

    static final String FILE_NAME_FORMAT = "data%05d%s.tar";

    @Nonnull
    final SegmentTracker tracker;

    @Nonnull
    final CachingSegmentReader segmentReader;

    final File directory;

    private final BlobStore blobStore;

    final boolean memoryMapping;

    @Nonnull
    final SegmentCache segmentCache;

    final TarRecovery recovery = new TarRecovery() {

        @Override
        public void recoverEntry(UUID uuid, byte[] data, TarWriter writer) throws IOException {
            writeSegment(uuid, data, writer);
        }

    };

    protected final IOMonitor ioMonitor;

    AbstractFileStore(final FileStoreBuilder builder) {
        this.directory = builder.getDirectory();
        this.tracker = new SegmentTracker(new SegmentIdFactory() {
            @Override @Nonnull
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(AbstractFileStore.this, msb, lsb);
            }
        });
        this.blobStore = builder.getBlobStore();
        this.segmentCache = new SegmentCache(builder.getSegmentCacheSize());
        this.segmentReader = new CachingSegmentReader(new Supplier<SegmentWriter>() {
            @Override
            public SegmentWriter get() {
                return getWriter();
            }
        }, blobStore, builder.getStringCacheSize(), builder.getTemplateCacheSize());
        this.memoryMapping = builder.getMemoryMapping();
        this.ioMonitor = builder.getIOMonitor();
    }

     File getManifestFile() {
        return new File(directory, MANIFEST_FILE_NAME);
    }

     Manifest openManifest() throws IOException {
        File file = getManifestFile();

        if (file.exists()) {
            return Manifest.load(file);
        }

        return null;
    }

     static Manifest checkManifest(Manifest manifest) throws InvalidFileStoreVersionException {
        if (manifest == null) {
            throw new InvalidFileStoreVersionException("Using oak-segment-tar, but oak-segment should be used");
        }

        int storeVersion = manifest.getStoreVersion(INVALID_STORE_VERSION);

        // A store version less than or equal to the highest invalid value means
        // that something or someone is messing up with the manifest. This error
        // is not recoverable and is thus represented as an ISE.

        if (storeVersion <= INVALID_STORE_VERSION) {
            throw new IllegalStateException("Invalid store version");
        }

        if (storeVersion < CURRENT_STORE_VERSION) {
            throw new InvalidFileStoreVersionException("Using a too recent version of oak-segment-tar");
        }

        if (storeVersion > CURRENT_STORE_VERSION) {
            throw new InvalidFileStoreVersionException("Using a too old version of oak-segment tar");
        }

        return manifest;
    }

    @Nonnull
    public CacheStatsMBean getSegmentCacheStats() {
        return segmentCache.getCacheStats();
    }

    @Nonnull
    public CacheStatsMBean getStringCacheStats() {
        return segmentReader.getStringCacheStats();
    }

    @Nonnull
    public CacheStatsMBean getTemplateCacheStats() {
        return segmentReader.getTemplateCacheStats();
    }

    static Map<Integer, Map<Character, File>> collectFiles(File directory) {
        Map<Integer, Map<Character, File>> dataFiles = newHashMap();

        for (File file : listFiles(directory)) {
            Matcher matcher = FILE_NAME_PATTERN.matcher(file.getName());
            if (matcher.matches()) {
                Integer index = Integer.parseInt(matcher.group(2));
                Map<Character, File> files = dataFiles.get(index);
                if (files == null) {
                    files = newHashMap();
                    dataFiles.put(index, files);
                }
                Character generation = 'a';
                if (matcher.group(4) != null) {
                    generation = matcher.group(4).charAt(0);
                }
                checkState(files.put(generation, file) == null);
            }
        }

        return dataFiles;
    }

    @Nonnull
    private static File[] listFiles(File directory) {
        File[] files = directory.listFiles();
        return files == null ? new File[] {} : files;
    }

    @Nonnull
    public abstract SegmentWriter getWriter();

    @Nonnull
    public SegmentReader getReader() {
        return segmentReader;
    }

    @Nonnull
    public SegmentIdProvider getSegmentIdProvider() {
        return tracker;
    }

    /**
     * @return the {@link Revisions} object bound to the current store.
     */
    public abstract Revisions getRevisions();

    /**
     * Convenience method for accessing the root node for the current head.
     * This is equivalent to
     * <pre>
     * fileStore.getReader().readHeadState(fileStore.getRevisions())
     * </pre>
     * @return the current head node state
     */
    @Nonnull
    public SegmentNodeState getHead() {
        return segmentReader.readHeadState(getRevisions());
    }

    /**
     * @return  the external BlobStore (if configured) with this store, {@code null} otherwise.
     */
    @CheckForNull
    public BlobStore getBlobStore() {
        return blobStore;
    }

    private void writeSegment(UUID id, byte[] data, TarWriter w) throws IOException {
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int generation = Segment.getGcGeneration(buffer, id);
        w.writeEntry(msb, lsb, data, 0, data.length, generation);
        if (SegmentId.isDataSegmentId(lsb)) {
            Segment segment = new Segment(tracker, segmentReader, tracker.newSegmentId(msb, lsb), buffer);
            populateTarGraph(segment, w);
            populateTarBinaryReferences(segment, w);
        }
    }

    static void populateTarGraph(Segment segment, TarWriter w) {
        UUID from = segment.getSegmentId().asUUID();
        for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
            w.addGraphEdge(from, segment.getReferencedSegmentId(i));
        }
    }

    static void populateTarBinaryReferences(final Segment segment, final TarWriter w) {
        final int generation = segment.getGcGeneration();
        final UUID id = segment.getSegmentId().asUUID();
        segment.forEachRecord(new RecordConsumer() {

            @Override
            public void consume(int number, RecordType type, int offset) {
                if (type == RecordType.BLOB_ID) {
                    w.addBinaryReference(generation, id, SegmentBlob.readBlobId(segment, number));
                }
            }

        });
    }

    static void closeAndLogOnFail(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ioe) {
                // ignore and log
                log.error(ioe.getMessage(), ioe);
            }
        }
    }

}
