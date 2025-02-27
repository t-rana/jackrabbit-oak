/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.file.tar;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;

import static java.util.Collections.emptySet;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.guava.common.collect.Iterables;

import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.collections.ListUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.file.FileReaper;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TarFiles implements Closeable {

    private static class Node {

        final TarReader reader;

        final Node next;

        Node(TarReader reader, Node next) {
            this.reader = reader;
            this.next = next;
        }

    }

    public static class CleanupResult {

        private boolean interrupted;

        private long reclaimedSize;

        private List<String> removableFiles;

        private Set<UUID> reclaimedSegmentIds;

        private CleanupResult() {
            // Prevent external instantiation.
        }

        public long getReclaimedSize() {
            return reclaimedSize;
        }

        public List<String> getRemovableFiles() {
            return removableFiles;
        }

        public Set<UUID> getReclaimedSegmentIds() {
            return reclaimedSegmentIds;
        }

        public boolean isInterrupted() {
            return interrupted;
        }

    }

    public static class Builder {

        private File directory;

        private boolean memoryMapping;

        private boolean offHeapAccess;

        private TarRecovery tarRecovery;

        private IOMonitor ioMonitor;

        private FileStoreMonitor fileStoreMonitor;

        private RemoteStoreMonitor remoteStoreMonitor;

        private long maxFileSize;

        private boolean readOnly;

        private SegmentNodeStorePersistence persistence;

        private CounterStats readerCountStats = NoopStats.INSTANCE;

        private CounterStats segmentCountStats = NoopStats.INSTANCE;

        private boolean initialiseReadersAndWriters = true;

        private Builder() {
            // Prevent external instantiation.
        }

        public Builder withDirectory(File directory) {
            this.directory = requireNonNull(directory);
            return this;
        }

        public Builder withMemoryMapping(boolean memoryMapping) {
            this.memoryMapping = memoryMapping;
            return this;
        }

        public Builder withOffHeapAccess(boolean offHeapAccess) {
            this.offHeapAccess = offHeapAccess;
            return this;
        }

        public Builder withTarRecovery(TarRecovery tarRecovery) {
            this.tarRecovery = requireNonNull(tarRecovery);
            return this;
        }

        public Builder withIOMonitor(IOMonitor ioMonitor) {
            this.ioMonitor = requireNonNull(ioMonitor);
            return this;
        }

        public Builder withFileStoreMonitor(FileStoreMonitor fileStoreStats) {
            this.fileStoreMonitor = requireNonNull(fileStoreStats);
            return this;
        }

        public Builder withRemoteStoreMonitor(RemoteStoreMonitor remoteStoreMonitor) {
            this.remoteStoreMonitor = requireNonNull(remoteStoreMonitor);
            return this;
        }

        public Builder withMaxFileSize(long maxFileSize) {
            checkArgument(maxFileSize > 0);
            this.maxFileSize = maxFileSize;
            return this;
        }

        public Builder withReadOnly() {
            this.readOnly = true;
            return this;
        }

        public Builder withPersistence(SegmentNodeStorePersistence persistence) {
            this.persistence = persistence;
            return this;
        }

        public Builder withReaderCountStats(CounterStats readerCountStats) {
            this.readerCountStats = readerCountStats;
            return this;
        }

        public Builder withSegmentCountStats(CounterStats segmentCountStats) {
            this.segmentCountStats = segmentCountStats;
            return this;
        }

        public Builder withInitialisedReadersAndWriters(boolean initialiseReadersAndWriters) {
            this.initialiseReadersAndWriters = initialiseReadersAndWriters;
            return this;
        }

        public TarFiles build() throws IOException {
            Validate.checkState(directory != null, "Directory not specified");
            Validate.checkState(tarRecovery != null, "TAR recovery strategy not specified");
            Validate.checkState(ioMonitor != null, "I/O monitor not specified");
            Validate.checkState(readOnly || fileStoreMonitor != null, "File store statistics not specified");
            Validate.checkState(remoteStoreMonitor != null, "Remote store statistics not specified");
            Validate.checkState(readOnly || maxFileSize != 0, "Max file size not specified");
            if (persistence == null) {
                persistence = new TarPersistence(directory);
            }
            return new TarFiles(this);
        }

        public File getDirectory() {
            return directory;
        }

        public boolean isMemoryMapping() {
            return memoryMapping;
        }

        public TarRecovery getTarRecovery() {
            return tarRecovery;
        }

        public IOMonitor getIoMonitor() {
            return ioMonitor;
        }

        public FileStoreMonitor getFileStoreMonitor() {
            return fileStoreMonitor;
        }

        public RemoteStoreMonitor getRemoteStoreMonitor() {
            return remoteStoreMonitor;
        }

        public long getMaxFileSize() {
            return maxFileSize;
        }

        public boolean isReadOnly() {
            return readOnly;
        }

        private SegmentArchiveManager buildArchiveManager() throws IOException {
            return persistence.createArchiveManager(memoryMapping, offHeapAccess, ioMonitor, readOnly && fileStoreMonitor == null ? new FileStoreMonitorAdapter() : fileStoreMonitor, remoteStoreMonitor);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TarFiles.class);

    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("(data)((0|[1-9][0-9]*)[0-9]{4})([a-z])?.tar");

    private static Node reverse(Node n) {
        Node r = null;
        while (n != null) {
            r = new Node(n.reader, r);
            n = n.next;
        }
        return r;
    }

    private static Iterable<TarReader> iterable(final Node head) {
        return new Iterable<TarReader>() {

            @NotNull
            @Override
            public Iterator<TarReader> iterator() {
                return new Iterator<TarReader>() {

                    private Node next = head;

                    @Override
                    public boolean hasNext() {
                        return next != null;
                    }

                    @Override
                    public TarReader next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        Node current = next;
                        next = current.next;
                        return current.reader;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("not implemented");
                    }

                };
            }

        };
    }

    private static Map<Integer, Map<Character, String>> collectFiles(SegmentArchiveManager archiveManager) throws IOException {
        Map<Integer, Map<Character, String>> dataFiles = new HashMap<>();
        for (String file : archiveManager.listArchives()) {
            Matcher matcher = FILE_NAME_PATTERN.matcher(file);
            if (matcher.matches()) {
                Integer index = Integer.parseInt(matcher.group(2));
                Map<Character, String> files = dataFiles.get(index);
                if (files == null) {
                    files = new HashMap<>();
                    dataFiles.put(index, files);
                }
                Character generation = 'a';
                if (matcher.group(4) != null) {
                    generation = matcher.group(4).charAt(0);
                }
                Validate.checkState(files.put(generation, file) == null);
            }
        }
        return dataFiles;
    }

    public static Builder builder() {
        return new Builder();
    }

    private final long maxFileSize;

    private SegmentArchiveManager archiveManager;

    /**
     * Guards access to the {@link #readers} and {@link #writer} references.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Points to the first node of the linked list of TAR readers. Every node in
     * the linked list is immutable. Thus, you need to to hold {@link #lock}
     * while reading the value of the reference, but you can release it before
     * iterating through the list.
     * <p>
     * Please note that while the linked list is immutable, the pointer to it
     * (namely this instance variable) is not itself immutable. This reference
     * must be kept consistent with {@link #writer}, and this is the reason why
     * it's necessary to hold a lock while accessing this variable.
     */
    private Node readers;

    /**
     * The currently used TAR writer. Its access is protected by {@link #lock}.
     */
    private TarWriter writer;

    /**
     * If {@code true}, a user requested this instance to close. This flag is
     * used in long running, background operations - like {@link
     * #cleanup(CleanupContext)} - to be responsive to termination.
     */
    private volatile boolean shutdown;

    /**
     * Counter exposing the number of {@link TarReader} instances.
     */
    private final CounterStats readerCount;

    /**
     * Counter exposing the number of segments.
     */
    private final CounterStats segmentCount;

    private final boolean readOnly;

    private final TarRecovery tarRecovery;

    /**
     * If {@code true}, the readers and writers are initialised.
     */
    private boolean initialised;

    private static int getSegmentCount(TarReader reader) {
        return reader.getEntries().length;
    }

    private TarFiles(Builder builder) throws IOException {
        maxFileSize = builder.maxFileSize;
        archiveManager = builder.buildArchiveManager();
        readerCount = builder.readerCountStats;
        segmentCount = builder.segmentCountStats;
        readOnly = builder.readOnly;
        tarRecovery = builder.tarRecovery;

        if (builder.initialiseReadersAndWriters) {
            init();
        }
    }

    public void init() throws IOException {
        Map<Integer, Map<Character, String>> map = collectFiles(archiveManager);
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);

        // TAR readers are stored in descending index order. The following loop
        // iterates the indices in ascending order, but prepends - instead of
        // appending - the corresponding TAR readers to the linked list. This
        // results in a properly ordered linked list.

        for (Integer index : indices) {
            TarReader r;
            if (readOnly) {
                r = TarReader.openRO(map.get(index), tarRecovery, archiveManager);
            } else {
                r = TarReader.open(map.get(index), tarRecovery, archiveManager);
            }
            segmentCount.inc(getSegmentCount(r));
            readers = new Node(r, readers);
            readerCount.inc();
        }
        if (!readOnly) {
            int writeNumber = 0;
            if (indices.length > 0) {
                writeNumber = indices[indices.length - 1] + 1;
            }
            writer = new TarWriter(archiveManager, writeNumber, segmentCount);
        }

        initialised = true;
    }

    private void checkInitialised() {
        if (!initialised) {
            throw new IllegalRepositoryStateException("TarFiles not initialised");
        }
    }

    @Override
    public void close() throws IOException {
        shutdown = true;

        TarWriter w;
        Node head;

        lock.writeLock().lock();
        try {
            w = writer;
            head = readers;
        } finally {
            lock.writeLock().unlock();
        }

        IOException exception = null;

        if (w != null) {
            try {
                w.close();
            } catch (IOException e) {
                exception = e;
            }
        }

        for (TarReader reader : iterable(head)) {
            try {
                reader.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public String toString() {
        String w = null;
        Node head;

        lock.readLock().lock();
        try {
            if (writer != null) {
                w = writer.toString();
            }
            head = readers;
        } finally {
            lock.readLock().unlock();
        }

        return String.format("TarFiles{readers=%s,writer=%s}", ListUtils.toList(iterable(head)), w);
    }

    public long size() {
        long size = 0;
        Node head;

        lock.readLock().lock();
        try {
            if (writer != null) {
                size = writer.fileLength();
            }
            head = readers;
        } finally {
            lock.readLock().unlock();
        }

        for (TarReader reader : iterable(head)) {
            size += reader.size();
        }
        return size;
    }

    private static int getSize(Node head) {
        return Iterables.size(iterable(head));
    }

    public int readerCount() {
        Node head;

        lock.readLock().lock();
        try {
            head = readers;
        } finally {
            lock.readLock().unlock();
        }

        return getSize(head);
    }

    /**
     * @return the number of segments in the segment store
     */
    public int segmentCount() {
        int count = 0;
        Node head;

        lock.readLock().lock();
        try {
            if (writer != null) {
                count = writer.getEntryCount();
            }
            head = readers;
        } finally {
            lock.readLock().unlock();
        }

        for (TarReader reader : iterable(head)) {
            count += getSegmentCount(reader);
        }
        return count;
    }

    public void flush() throws IOException {
        checkInitialised();
        lock.readLock().lock();
        try {
            writer.flush();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean containsSegment(long msb, long lsb) {
        Node head;

        lock.readLock().lock();
        try {
            if (writer != null) {
                if (writer.containsEntry(msb, lsb)) {
                    return true;
                }
            }
            head = readers;
        } finally {
            lock.readLock().unlock();
        }

        for (TarReader reader : iterable(head)) {
            if (reader.containsEntry(msb, lsb)) {
                return true;
            }
        }
        return false;
    }

    public Buffer readSegment(long msb, long lsb) {
        try {
            Node head;

            lock.readLock().lock();
            try {
                if (writer != null) {
                    Buffer b = writer.readEntry(msb, lsb);
                    if (b != null) {
                        return b;
                    }
                }
                head = readers;
            } finally {
                lock.readLock().unlock();
            }

            for (TarReader reader : iterable(head)) {
                Buffer b = reader.readEntry(msb, lsb);
                if (b != null) {
                    return b;
                }
            }
        } catch (IOException e) {
            log.warn("Unable to read from TAR file", e);
        }

        return null;
    }

    public void writeSegment(UUID id, byte[] buffer, int offset, int length, GCGeneration generation, Set<UUID> references, Set<String> binaryReferences) throws IOException {
        checkInitialised();
        lock.writeLock().lock();
        try {
            long size = writer.writeEntry(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits(),
                    buffer,
                    offset,
                    length,
                    generation
            );
            if (references != null) {
                for (UUID reference : references) {
                    writer.addGraphEdge(id, reference);
                }
            }
            if (binaryReferences != null) {
                for (String reference : binaryReferences) {
                    writer.addBinaryReference(generation, id, reference);
                }
            }
            int entryCount = writer.getEntryCount();
            if (size >= maxFileSize || entryCount >= writer.getMaxEntryCount()) {
                internalNewWriter();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Creates a new TAR writer with a higher index number, reopens the previous
     * TAR writer as a TAR reader, and adds the TAR reader to the linked list.
     * <p>
     * This method must be invoked while holding {@link #lock} in write mode,
     * because it modifies the references {@link #writer} and {@link #readers}.
     *
     * @throws IOException If an error occurs while operating on the TAR readers
     *                     or the TAR writer.
     */
    private void internalNewWriter() throws IOException {
        TarWriter newWriter = writer.createNextGeneration();
        if (newWriter == writer) {
            return;
        }
        TarReader reader = TarReader.open(writer.getFileName(), archiveManager);
        readers = new Node(reader, readers);
        segmentCount.inc(getSegmentCount(reader));
        readerCount.inc();
        writer = newWriter;
    }

    void newWriter() throws IOException {
        checkInitialised();
        lock.writeLock().lock();
        try {
            internalNewWriter();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public CleanupResult cleanup(CleanupContext context) throws IOException {
        checkInitialised();
        CleanupResult result = new CleanupResult();
        result.removableFiles = new ArrayList<>();
        result.reclaimedSegmentIds = new HashSet<>();

        Set<UUID> references;
        Node head;

        lock.writeLock().lock();
        lock.readLock().lock();
        try {
            try {
                internalNewWriter();
            } finally {
                lock.writeLock().unlock();
            }
            head = readers;
            references = new HashSet<>(context.initialReferences());
        } finally {
            lock.readLock().unlock();
        }

        Map<TarReader, TarReader> cleaned = new LinkedHashMap<>();

        for (TarReader reader : iterable(head)) {
            cleaned.put(reader, reader);
            result.reclaimedSize += reader.size();
        }

        Set<UUID> reclaim = new HashSet<>();

        for (TarReader reader : cleaned.keySet()) {
            if (shutdown) {
                result.interrupted = true;
                return result;
            }
            reader.mark(references, reclaim, context);
        }

        for (TarReader reader : cleaned.keySet()) {
            if (shutdown) {
                result.interrupted = true;
                return result;
            }
            cleaned.put(reader, reader.sweep(reclaim, result.reclaimedSegmentIds));
        }

        Node closeables;
        long reclaimed;

        Node swept;
        while (true) {
            closeables = null;
            reclaimed = 0;

            swept = null;

            // The following loops creates a modified version of `readers` and
            // saves it into `swept`. Some TAR readers in `readers` have been
            // swept by the previous code and must be replaced with a slimmer
            // TAR reader with the same index but a higher generation.

            for (TarReader reader : iterable(head)) {
                if (cleaned.containsKey(reader)) {

                    // We distinguish three cases. First, the original TAR
                    // reader is unmodified. This happens with no content or not
                    // enough content could be swept from the original TAR
                    // reader. Second, some content could be swept from the
                    // original TAR reader and a new TAR reader with the same
                    // index and a higher generation was created. Third, all the
                    // content from the original TAR reader could be swept.

                    TarReader cleanedReader = cleaned.get(reader);
                    if (cleanedReader != null) {

                        // We are either in the first or in the second case.
                        // Save the TAR reader (either the original or the one
                        // with a higher generation) in the resulting linked list.

                        swept = new Node(cleanedReader, swept);
                        reclaimed += cleanedReader.size();
                    }

                    if (cleanedReader != reader) {

                        // We are either in the second or third case. Save the
                        // original TAR reader in a list of TAR readers that
                        // will be closed at the end of this methods.

                        closeables = new Node(reader, closeables);
                    }
                } else {

                    // This reader was not involved in the mark-and-sweep. This
                    // might happen in iterations of this loop successive to the
                    // first, when we re-read `readers` and recompute `swept`
                    // all over again.

                    swept = new Node(reader, swept);
                }
            }

            // `swept` is in the reverse order because we prepended new nodes
            // to it. We have to reverse it before we save it into `readers`.

            swept = reverse(swept);

            // Following is a compare-and-set operation. We based the
            // computation of `swept` of a specific value of `readers`. If
            // `readers` is still the same as the one we started with, we just
            // update `readers` and exit from the loop. Otherwise, we read the
            // value of `readers` and recompute `swept` based on this value.

            lock.writeLock().lock();
            try {
                if (readers == head) {
                    readers = swept;
                    break;
                } else {
                    head = readers;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        readerCount.dec(getSize(head) - getSize(swept));
        segmentCount.dec(getSegmentCount(head) - getSegmentCount(swept));

        result.reclaimedSize -= reclaimed;

        for (TarReader closeable : iterable(closeables)) {
            try {
                closeable.close();
            } catch (IOException e) {
                log.warn("Unable to close swept TAR reader", e);
            }
            result.removableFiles.add(closeable.getFileName());
        }

        return result;
    }

    private static int getSegmentCount(Node head) {
        int c = 0;
        for (TarReader reader : iterable(head)) {
            c += getSegmentCount(reader);
        }
        return c;
    }

    public void collectBlobReferences(Consumer<String> collector, Predicate<GCGeneration> reclaim) throws IOException {
        checkInitialised();
        Node head;
        lock.writeLock().lock();
        try {
            if (writer != null) {
                internalNewWriter();
            }
            head = readers;
        } finally {
            lock.writeLock().unlock();
        }

        for (TarReader reader : iterable(head)) {
            reader.collectBlobReferences(collector, reclaim);
        }
    }

    public Iterable<UUID> getSegmentIds() {
        Node head;

        lock.readLock().lock();
        try {
            head = readers;
        } finally {
            lock.readLock().unlock();
        }

        List<UUID> ids = new ArrayList<>();
        for (TarReader reader : iterable(head)) {
            ids.addAll(reader.getUUIDs());
        }
        return ids;
    }

    public Map<UUID, Set<UUID>> getGraph(String fileName) throws IOException {
        Node head;

        lock.readLock().lock();
        try {
            head = readers;
        } finally {
            lock.readLock().unlock();
        }

        Set<UUID> index = null;
        Map<UUID, List<UUID>> graph = null;

        for (TarReader reader : iterable(head)) {
            if (fileName.equals(reader.getFileName())) {
                index = reader.getUUIDs();
                graph = reader.getGraph();
                break;
            }
        }

        Map<UUID, Set<UUID>> result = new HashMap<>();
        if (index != null) {
            for (UUID uuid : index) {
                result.put(uuid, emptySet());
            }
        }
        if (graph != null) {
            for (Entry<UUID, List<UUID>> entry : graph.entrySet()) {
                result.put(entry.getKey(), new HashSet<>(entry.getValue()));
            }
        }
        return result;
    }

    public Map<String, Set<UUID>> getIndices() {
        Node head;

        lock.readLock().lock();
        try {
            head = readers;
        } finally {
            lock.readLock().unlock();
        }

        Map<String, Set<UUID>> index = new HashMap<>();
        for (TarReader reader : iterable(head)) {
            index.put(reader.getFileName(), reader.getUUIDs());
        }
        return index;
    }

    public FileReaper createFileReaper() {
        checkInitialised();
        return new FileReaper(archiveManager);
    }
}
