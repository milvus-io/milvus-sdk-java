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

package io.milvus.bulkwriter;

import io.milvus.bulkwriter.common.clientenum.UploadPolicy;
import io.milvus.bulkwriter.storage.StorageClient.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.FileSystemLoopException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** Request-local adaptive index. All access belongs to the upload coordinator thread. */
final class UploadManifest implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(UploadManifest.class);
    private static final long LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int COMMIT_INTERVAL = 1000;
    static final int MEMORY_FILE_LIMIT = 10_000;
    static final int MEMORY_DIRECTORY_LIMIT = 10_000;
    static final long MEMORY_ESTIMATE_LIMIT = 8L * 1024 * 1024;
    private final Path tempDirectory;
    private Path directory;
    private Connection connection;
    private PreparedStatement insert;
    private PreparedStatement enqueue;
    private PreparedStatement nextDirectory;
    private PreparedStatement removeDirectory;
    private Map<String, Entry> memoryFiles = new TreeMap<>();
    private Deque<String> memoryDirectories = new ArrayDeque<>();
    private long estimatedMemoryBytes;
    private long fileCount;
    private long totalBytes;
    private long remainingCount;
    private long unmatchedCount;

    UploadManifest(Path tempDirectory) throws IOException, SQLException {
        this.tempDirectory = tempDirectory;
    }

    private void spillToDisk(String reason) throws SQLException {
        try {
            directory = tempDirectory == null ? Files.createTempDirectory("milvus-volume-")
                    : Files.createTempDirectory(tempDirectory, "milvus-volume-");
        } catch (IOException e) {
            throw new SQLException("Cannot create temporary volume upload index", e);
        }
        Connection opened = null;
        try {
            opened = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("manifest.db"));
            try (Statement statement = opened.createStatement()) {
                statement.execute("PRAGMA cache_size = -8192");
                statement.execute("PRAGMA mmap_size = 0");
                statement.execute("PRAGMA temp_store = FILE");
                statement.execute("CREATE TABLE files (path TEXT PRIMARY KEY, size INTEGER NOT NULL, "
                        + "modified INTEGER NOT NULL, matched INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID");
                statement.execute("CREATE TABLE directories (id INTEGER PRIMARY KEY, path TEXT NOT NULL)");
            }
            opened.setAutoCommit(false);
            try (PreparedStatement copy = opened.prepareStatement(
                    "INSERT INTO files(path, size, modified, matched) VALUES (?, ?, ?, ?)");
                 PreparedStatement copyDirectory = opened.prepareStatement("INSERT INTO directories(path) VALUES (?)")) {
                for (Entry entry : memoryFiles.values()) {
                    checkInterrupted();
                    copy.setString(1, entry.relativePath);
                    copy.setLong(2, entry.size);
                    copy.setLong(3, entry.modified);
                    copy.setBoolean(4, entry.matched);
                    copy.executeUpdate();
                }
                for (String path : memoryDirectories) {
                    checkInterrupted();
                    copyDirectory.setString(1, path);
                    copyDirectory.executeUpdate();
                }
            }
            opened.commit();
            insert = opened.prepareStatement("INSERT INTO files(path, size, modified) VALUES (?, ?, ?)");
            enqueue = opened.prepareStatement("INSERT INTO directories(path) VALUES (?)");
            nextDirectory = opened.prepareStatement("SELECT id, path FROM directories ORDER BY id LIMIT 1");
            removeDirectory = opened.prepareStatement("DELETE FROM directories WHERE id = ?");
            connection = opened;
            memoryFiles = null;
            memoryDirectories = null;
            logger.info("Volume upload index switched to SQLite: reason:{}, scannedFiles:{}, estimatedMemoryBytes:{}",
                    reason, fileCount, estimatedMemoryBytes);
            estimatedMemoryBytes = 0;
        } catch (IOException | SQLException | RuntimeException | Error e) {
            if (opened != null) {
                try { opened.close(); } catch (SQLException suppressed) { e.addSuppressed(suppressed); }
            }
            try { deleteDirectory(); } catch (IOException suppressed) { e.addSuppressed(suppressed); }
            insert = null;
            enqueue = null;
            nextDirectory = null;
            removeDirectory = null;
            if (e instanceof IOException) {
                throw new SQLException("Volume upload index migration interrupted", e);
            }
            if (e instanceof SQLException) { throw (SQLException) e; }
            if (e instanceof RuntimeException) { throw (RuntimeException) e; }
            throw (Error) e;
        }
    }

    private void enqueueDirectory(String path) throws SQLException {
        long estimatedBytes = 64L + 2L * path.length();
        if (connection == null) {
            if (memoryDirectories.size() >= MEMORY_DIRECTORY_LIMIT) {
                spillToDisk("pending directory count exceeds " + MEMORY_DIRECTORY_LIMIT);
            } else if (estimatedMemoryBytes + estimatedBytes > MEMORY_ESTIMATE_LIMIT) {
                spillToDisk("estimated index memory exceeds " + MEMORY_ESTIMATE_LIMIT);
            }
        }
        if (connection == null) {
            memoryDirectories.addLast(path);
            estimatedMemoryBytes += estimatedBytes;
        } else {
            enqueue.setString(1, path);
            enqueue.executeUpdate();
        }
    }

    private String pollDirectory() throws SQLException {
        if (connection == null) {
            String path = memoryDirectories.pollFirst();
            if (path != null) { estimatedMemoryBytes -= 64L + 2L * path.length(); }
            return path;
        }
        long id;
        String path;
        try (ResultSet row = nextDirectory.executeQuery()) {
            if (!row.next()) { return null; }
            id = row.getLong(1);
            path = row.getString(2);
        }
        removeDirectory.setLong(1, id);
        removeDirectory.executeUpdate();
        return path;
    }

    /** Follows file/directory links using their logical upload paths; rejects cycles and unsupported entries. */
    boolean scan(Path root) throws IOException, SQLException {
        root = root.toAbsolutePath().normalize();
        long startedAt = System.nanoTime();
        long lastLogAt = startedAt;
        long scannedDirectories = 0;
        logger.info("Volume local scan started: sourcePath:{}", root);
        BasicFileAttributes attributes = Files.readAttributes(root, BasicFileAttributes.class);
        if (attributes.isRegularFile()) {
            add(root.getFileName().toString(), attributes.size(), attributes.lastModifiedTime().toMillis());
            finishScan();
            logScanProgress(root, scannedDirectories, startedAt, lastLogAt, true);
            return true;
        }
        if (!attributes.isDirectory()) {
            throw new IOException("Upload source must be a regular file or directory: " + root);
        }
        enqueueDirectory("");
        long scannedEntries = 0;
        while (true) {
            checkInterrupted();
            String relativeDirectory = pollDirectory();
            if (relativeDirectory == null) { break; }
            Path current = root.resolve(relativeDirectory);
            BasicFileAttributes currentAttributes = Files.readAttributes(current, BasicFileAttributes.class);
            if (!currentAttributes.isDirectory()) {
                throw new IOException("Upload directory changed during scan: " + current);
            }
            checkDirectoryCycle(root, current);
            try (DirectoryStream<Path> children = Files.newDirectoryStream(current)) {
                for (Path child : children) {
                    checkInterrupted();
                    if (directory != null && child.getFileName().equals(directory.getFileName())
                            && Files.isSameFile(child, directory)) { continue; }
                    BasicFileAttributes attrs = Files.readAttributes(child, BasicFileAttributes.class);
                    String relative = root.relativize(child).toString().replace(child.getFileSystem().getSeparator(), "/");
                    if (attrs.isRegularFile()) {
                        add(relative, attrs.size(), attrs.lastModifiedTime().toMillis());
                    } else if (attrs.isDirectory()) {
                        enqueueDirectory(relative);
                    } else {
                        throw new IOException("Special files are not supported: " + child);
                    }
                    if (++scannedEntries % COMMIT_INTERVAL == 0) { finishScan(); }
                    lastLogAt = logScanProgress(root, scannedDirectories, startedAt, lastLogAt, false);
                }
            }
            if (++scannedEntries % COMMIT_INTERVAL == 0) { finishScan(); }
            scannedDirectories++;
            lastLogAt = logScanProgress(root, scannedDirectories, startedAt, lastLogAt, false);
        }
        finishScan();
        logScanProgress(root, scannedDirectories, startedAt, lastLogAt, true);
        return false;
    }

    private void checkDirectoryCycle(Path root, Path current) throws IOException {
        if (current.equals(root) || !Files.isSymbolicLink(current)) { return; }
        Path target = current.toRealPath();
        // Only inspect this traversal's ancestors, so separate aliases to the same directory
        // are still uploaded under their own keys. No unbounded visited-directory set is needed.
        for (Path ancestor = current.getParent(); ancestor != null; ancestor = ancestor.getParent()) {
            checkInterrupted();
            // A target containing an ancestor also loops when traversal reaches that ancestor.
            if (ancestor.toRealPath().startsWith(target) || Files.isSameFile(current, ancestor)) {
                throw new FileSystemLoopException(current.toString());
            }
            if (ancestor.equals(root)) { break; }
        }
    }

    private long logScanProgress(Path root, long scannedDirectories, long startedAt, long lastLogAt,
                                 boolean completed) {
        long now = System.nanoTime();
        if (completed || now - lastLogAt >= LOG_INTERVAL_NANOS) {
            logger.info("Volume local scan {}: sourcePath:{}, scannedDirectories:{}, scannedFiles:{}, scannedBytes:{}, elapsedMillis:{}, indexStorage:{}",
                    completed ? "completed" : "progress", root, scannedDirectories, fileCount, totalBytes,
                    TimeUnit.NANOSECONDS.toMillis(now - startedAt), connection == null ? "memory" : "sqlite");
            return now;
        }
        return lastLogAt;
    }

    void add(String relativePath, long size, long modified) throws SQLException {
        long estimatedBytes = 192L + 2L * relativePath.length();
        if (connection == null) {
            if (memoryFiles.containsKey(relativePath)) {
                throw new SQLException("Duplicate local upload path: " + relativePath);
            }
            if (fileCount >= MEMORY_FILE_LIMIT) {
                spillToDisk("file count exceeds " + MEMORY_FILE_LIMIT);
            } else if (estimatedMemoryBytes + estimatedBytes > MEMORY_ESTIMATE_LIMIT) {
                spillToDisk("estimated index memory exceeds " + MEMORY_ESTIMATE_LIMIT);
            }
        }
        if (connection == null) {
            memoryFiles.put(relativePath, new Entry(relativePath, size, modified));
            estimatedMemoryBytes += estimatedBytes;
        } else {
            insert.setString(1, relativePath);
            insert.setLong(2, size);
            insert.setLong(3, modified);
            insert.executeUpdate();
        }
        fileCount++;
        totalBytes += size;
        remainingCount++;
        unmatchedCount++;
        if (fileCount % COMMIT_INTERVAL == 0) { finishScan(); }
    }

    void finishScan() throws SQLException {
        if (connection != null) { connection.commit(); }
    }

    void excludeExisting(List<ObjectMetadata> objects, String targetPrefix, UploadPolicy policy)
            throws SQLException, InterruptedIOException {
        if (connection == null) {
            for (ObjectMetadata object : objects) {
                checkInterrupted();
                if (!object.getKey().startsWith(targetPrefix)) { continue; }
                String path = object.getKey().substring(targetPrefix.length());
                Entry entry = memoryFiles.get(path);
                if (entry == null) { continue; }
                if (!entry.matched) { unmatchedCount--; }
                entry.matched = true;
                if (policy.shouldSkip(entry.size, entry.modified, object.getSize(), object.getLastModifiedTimeMillis())) {
                    memoryFiles.remove(path);
                    estimatedMemoryBytes -= 192L + 2L * path.length();
                    remainingCount--;
                }
            }
            return;
        }
        try (PreparedStatement lookup = connection.prepareStatement("SELECT size, modified, matched FROM files WHERE path = ?");
             PreparedStatement delete = connection.prepareStatement("DELETE FROM files WHERE path = ?");
             PreparedStatement mark = connection.prepareStatement("UPDATE files SET matched = 1 WHERE path = ?")) {
            for (ObjectMetadata object : objects) {
                checkInterrupted();
                if (!object.getKey().startsWith(targetPrefix)) { continue; }
                String relativePath = object.getKey().substring(targetPrefix.length());
                lookup.setString(1, relativePath);
                boolean skip;
                boolean matched;
                try (ResultSet row = lookup.executeQuery()) {
                    if (!row.next()) { continue; }
                    matched = row.getBoolean(3);
                    skip = policy.shouldSkip(row.getLong(1), row.getLong(2),
                            object.getSize(), object.getLastModifiedTimeMillis());
                }
                if (!matched) { unmatchedCount--; }
                if (skip) {
                    delete.setString(1, relativePath);
                    remainingCount -= delete.executeUpdate();
                } else if (!matched) {
                    mark.setString(1, relativePath);
                    mark.executeUpdate();
                }
            }
            connection.commit();
        }
    }

    long remainingBytes() throws SQLException {
        if (connection == null) {
            long bytes = 0;
            for (Entry entry : memoryFiles.values()) { bytes += entry.size; }
            return bytes;
        }
        try (Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery("SELECT COALESCE(SUM(size), 0) FROM files")) {
            row.next();
            return row.getLong(1);
        }
    }

    Cursor openCursor() throws SQLException {
        return connection == null ? new Cursor(memoryFiles.values().iterator()) : new Cursor(connection);
    }

    long fileCount() { return fileCount; }
    long totalBytes() { return totalBytes; }
    long remainingCount() { return remainingCount; }
    long unmatchedCount() { return unmatchedCount; }
    Path directory() { return directory; }

    @Override
    public void close() throws IOException, SQLException {
        try {
            if (connection != null) {
                // Closing the connection also closes its prepared statements, including the queue.
                connection.close();
                connection = null;
            }
        } finally {
            if (memoryFiles != null) { memoryFiles.clear(); }
            if (memoryDirectories != null) { memoryDirectories.clear(); }
            deleteDirectory();
        }
    }

    private void deleteDirectory() throws IOException {
        if (directory == null) { return; }
        try (DirectoryStream<Path> children = Files.newDirectoryStream(directory)) {
            for (Path child : children) { Files.delete(child); }
        }
        Files.delete(directory);
        directory = null;
    }

    static void checkInterrupted() throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Volume upload interrupted");
        }
    }

    static final class Entry {
        final String relativePath;
        final long size;
        final long modified;
        private boolean matched;

        Entry(String relativePath, long size, long modified) {
            this.relativePath = relativePath;
            this.size = size;
            this.modified = modified;
        }
    }

    static final class Cursor implements AutoCloseable {
        private final Statement statement;
        private final ResultSet rows;
        private final Iterator<Entry> entries;

        Cursor(Iterator<Entry> entries) {
            this.entries = entries;
            statement = null;
            rows = null;
        }

        Cursor(Connection connection) throws SQLException {
            entries = null;
            statement = connection.createStatement();
            try {
                rows = statement.executeQuery("SELECT path, size, modified FROM files ORDER BY path");
            } catch (SQLException e) {
                statement.close();
                throw e;
            }
        }

        Entry next() throws SQLException, InterruptedIOException {
            checkInterrupted();
            if (entries != null) { return entries.hasNext() ? entries.next() : null; }
            return rows.next() ? new Entry(rows.getString(1), rows.getLong(2), rows.getLong(3)) : null;
        }

        @Override
        public void close() throws SQLException {
            if (entries != null) { return; }
            try { rows.close(); } finally { statement.close(); }
        }
    }
}
