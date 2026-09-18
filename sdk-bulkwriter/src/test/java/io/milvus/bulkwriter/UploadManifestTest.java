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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import io.milvus.bulkwriter.common.clientenum.UploadPolicy;
import io.milvus.bulkwriter.storage.StorageClient.ObjectMetadata;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class UploadManifestTest {
    @TempDir Path directory;

    @Test
    void nestedScanExactMatchesAndCleanup() throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Files.createDirectories(source.resolve("a/b/c"));
        Files.write(source.resolve("a/b/c/中文 + %.txt"), new byte[3]);
        Files.write(source.resolve("same"), new byte[5]);
        Files.write(source.resolve("same-more"), new byte[7]);
        try (UploadManifest manifest = new UploadManifest(directory)) {
            assertFalse(manifest.scan(source));
            assertNull(manifest.directory(), "Small uploads must not create a SQLite index");
            assertEquals(3, manifest.fileCount());
            assertEquals(15, manifest.totalBytes());
            manifest.excludeExisting(Arrays.asList("target/same", "target/same", "other/same-more",
                    "target/a/b/c/中文 + %.txt").stream().map(key -> new ObjectMetadata(key, null, null))
                    .collect(java.util.stream.Collectors.toList()), "target/", UploadPolicy.SKIP_IF_EXISTS);
            assertEquals(1, manifest.remainingCount());
            assertEquals(7, manifest.remainingBytes());
            try (UploadManifest.Cursor cursor = manifest.openCursor()) {
                assertEquals("same-more", cursor.next().relativePath);
                assertNull(cursor.next());
            }
        }
        try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
            assertEquals(1, paths.count(), "Only the source directory should exist");
        }
    }

    @Test
    void duplicateMismatchedKeysDoNotPrematurelyFinishTheScan() throws Exception {
        try (UploadManifest manifest = new UploadManifest(directory)) {
            manifest.add("a", 5, 1000);
            manifest.add("b", 5, 1000);
            manifest.finishScan();
            manifest.excludeExisting(Arrays.asList(new ObjectMetadata("target/a", 4L, 1000L),
                    new ObjectMetadata("target/a", 4L, 1000L)), "target/", UploadPolicy.SKIP_IF_SAME_SIZE);
            assertEquals(2, manifest.remainingCount());
            assertEquals(1, manifest.unmatchedCount());
            manifest.excludeExisting(Arrays.asList(new ObjectMetadata("target/b", 5L, 1000L)),
                    "target/", UploadPolicy.SKIP_IF_SAME_SIZE);
            assertEquals(1, manifest.remainingCount());
            assertEquals(0, manifest.unmatchedCount());
            assertEquals(5, manifest.remainingBytes());
            try (UploadManifest.Cursor cursor = manifest.openCursor()) {
                assertEquals("a", cursor.next().relativePath);
                assertNull(cursor.next());
            }
        }
    }

    @Test
    void temporaryIndexInsideSourceIsNotUploaded() throws Exception {
        Files.write(directory.resolve("file"), new byte[1]);
        try (UploadManifest manifest = new UploadManifest(directory)) {
            manifest.scan(directory);
            assertEquals(1, manifest.fileCount());
        }
    }

    @Test
    void missingSourceFailsAndCleansUp() throws Exception {
        try (UploadManifest manifest = new UploadManifest(directory)) {
            assertThrows(IOException.class, () -> manifest.scan(directory.resolve("missing")));
            assertNull(manifest.directory());
        }
        try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
            assertEquals(0, paths.count());
        }
    }

    @Test
    void fileThresholdMigratesEveryEntryAndMetadataWithoutChangingTotals() throws Exception {
        Path index;
        try (UploadManifest manifest = new UploadManifest(directory)) {
            for (int i = 0; i < UploadManifest.MEMORY_FILE_LIMIT; i++) {
                manifest.add(String.format("file-%05d", i), i, 1000 + i);
            }
            assertNull(manifest.directory(), "Exactly 10,000 files should stay in memory");
            manifest.add("file-10000", 10000, 11000);
            manifest.finishScan();
            index = manifest.directory();
            assertNotNull(index);
            assertTrue(Files.exists(index.resolve("manifest.db")));
            assertEquals(10001, manifest.fileCount());
            assertEquals(10001, manifest.remainingCount());
            assertEquals(10001, manifest.unmatchedCount());
            assertEquals(50005000, manifest.totalBytes());
            assertEquals(manifest.totalBytes(), manifest.remainingBytes());
            try (UploadManifest.Cursor cursor = manifest.openCursor()) {
                for (int i = 0; i <= 10000; i++) {
                    UploadManifest.Entry entry = cursor.next();
                    assertEquals(String.format("file-%05d", i), entry.relativePath);
                    assertEquals(i, entry.size);
                    assertEquals(1000 + i, entry.modified);
                }
                assertNull(cursor.next());
            }
        }
        assertFalse(Files.exists(index));
    }

    @Test
    void policiesProduceTheSameResultsInMemoryAndOnDisk() throws Exception {
        java.util.List<ObjectMetadata> remote = Arrays.asList(
                new ObjectMetadata("target/a", 5L, 2000L),
                new ObjectMetadata("target/b", 4L, 2000L),
                new ObjectMetadata("target/b", 4L, 2000L),
                new ObjectMetadata("target/c", 5L, 999L),
                new ObjectMetadata("target/d", null, null));
        for (UploadPolicy policy : UploadPolicy.values()) {
            for (boolean disk : new boolean[]{false, true}) {
                try (UploadManifest manifest = new UploadManifest(directory)) {
                    for (String path : Arrays.asList("a", "b", "c", "d", "e")) { manifest.add(path, 5, 1000); }
                    if (disk) {
                        java.util.List<ObjectMetadata> padding = new java.util.ArrayList<>();
                        for (int i = 0; i < UploadManifest.MEMORY_FILE_LIMIT; i++) {
                            manifest.add("padding-" + i, 0, 0);
                            padding.add(new ObjectMetadata("target/padding-" + i, 0L, 0L));
                        }
                        manifest.excludeExisting(padding, "target/", UploadPolicy.SKIP_IF_EXISTS);
                        assertNotNull(manifest.directory());
                    } else { assertNull(manifest.directory()); }
                    manifest.finishScan();
                    manifest.excludeExisting(remote, "target/", policy);
                    int skipped = policy == UploadPolicy.OVERWRITE ? 0 : policy == UploadPolicy.SKIP_IF_EXISTS ? 4
                            : policy == UploadPolicy.SKIP_IF_SAME_SIZE ? 2 : 1;
                    assertEquals(5 - skipped, manifest.remainingCount(), policy + " disk=" + disk);
                    assertEquals(5L * (5 - skipped), manifest.remainingBytes());
                    assertEquals(1, manifest.unmatchedCount());
                    // Repeated pages must not count the same keys again.
                    manifest.excludeExisting(remote, "target/", policy);
                    assertEquals(5 - skipped, manifest.remainingCount());
                    assertEquals(1, manifest.unmatchedCount());
                }
            }
        }
    }

    @Test
    void wideDirectoryQueueMigratesAndDoesNotIncludeItsOwnIndex() throws Exception {
        for (int i = 0; i <= UploadManifest.MEMORY_DIRECTORY_LIMIT; i++) {
            Path child = Files.createDirectory(directory.resolve("dir-" + i));
            if (i % 1000 == 0) { Files.write(child.resolve("file"), new byte[1]); }
        }
        Path index;
        try (UploadManifest manifest = new UploadManifest(directory)) {
            manifest.scan(directory);
            index = manifest.directory();
            assertNotNull(index, "Many pending empty directories must also trigger spill");
            assertEquals(11, manifest.fileCount());
            assertEquals(11, manifest.totalBytes());
            int files = 0;
            try (UploadManifest.Cursor cursor = manifest.openCursor()) {
                UploadManifest.Entry entry;
                while ((entry = cursor.next()) != null) {
                    assertTrue(Files.isRegularFile(directory.resolve(entry.relativePath)));
                    assertTrue(entry.relativePath.endsWith("/file"));
                    files++;
                }
            }
            assertEquals(11, files);
        }
        assertFalse(Files.exists(index));
    }

    @Test
    void longPathsTriggerMemoryGuardBeforeFileCountLimit() throws Exception {
        char[] chars = new char[4096];
        Arrays.fill(chars, 'x');
        String prefix = new String(chars);
        Path index;
        try (UploadManifest manifest = new UploadManifest(directory)) {
            int added = 0;
            while (manifest.directory() == null && added < UploadManifest.MEMORY_FILE_LIMIT) {
                manifest.add(prefix + added++, 1, 0);
            }
            assertTrue(added < UploadManifest.MEMORY_FILE_LIMIT);
            index = manifest.directory();
            assertNotNull(index);
            assertEquals(added, manifest.remainingCount());
            assertEquals(added, manifest.remainingBytes());
        }
        assertFalse(Files.exists(index));
    }

    @Test
    void smallUploadDoesNotNeedScratchDirectoryAndFailedSpillPreservesTheIndex() throws Exception {
        Path missing = directory.resolve("missing");
        try (UploadManifest manifest = new UploadManifest(missing)) {
            for (int i = 0; i < UploadManifest.MEMORY_FILE_LIMIT; i++) { manifest.add("file-" + i, 1, 0); }
            assertNull(manifest.directory());
            assertThrows(java.sql.SQLException.class, () -> manifest.add("overflow", 1, 0));
            assertEquals(UploadManifest.MEMORY_FILE_LIMIT, manifest.fileCount());
            assertEquals(UploadManifest.MEMORY_FILE_LIMIT, manifest.remainingBytes());
        }
        assertFalse(Files.exists(missing));
    }

    @Test
    void interruptedMigrationRemovesTemporaryDatabase() throws Exception {
        try (UploadManifest manifest = new UploadManifest(directory)) {
            for (int i = 0; i < UploadManifest.MEMORY_FILE_LIMIT; i++) { manifest.add("file-" + i, 1, 0); }
            Thread.currentThread().interrupt();
            try {
                assertThrows(java.sql.SQLException.class, () -> manifest.add("overflow", 1, 0));
                assertTrue(Thread.currentThread().isInterrupted());
            } finally { Thread.interrupted(); }
            assertNull(manifest.directory());
            assertEquals(UploadManifest.MEMORY_FILE_LIMIT, manifest.fileCount());
            try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
                assertEquals(0, paths.count());
            }
        }
    }

    @Test
    void directoryAliasesKeepLogicalKeysWithMemoryAndSqliteIndexes() throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path external = Files.createDirectories(directory.resolve("external/nested"));
        Files.write(external.resolve("data"), new byte[3]);
        Files.createSymbolicLink(source.resolve("first"), external.getParent());
        Files.createSymbolicLink(source.resolve("second"), external.getParent());
        for (boolean disk : new boolean[]{false, true}) {
            try (UploadManifest manifest = new UploadManifest(directory)) {
                if (disk) {
                    for (int i = 0; i <= UploadManifest.MEMORY_FILE_LIMIT; i++) {
                        manifest.add("padding-" + i, 0, 0);
                    }
                }
                assertFalse(manifest.scan(source));
                assertEquals(disk, manifest.directory() != null);
                assertEquals(6, manifest.totalBytes());
                try (UploadManifest.Cursor cursor = manifest.openCursor()) {
                    assertEquals("first/nested/data", cursor.next().relativePath);
                    UploadManifest.Entry entry;
                    do { entry = cursor.next(); } while (entry.relativePath.startsWith("padding-"));
                    assertEquals("second/nested/data", entry.relativePath);
                    assertNull(cursor.next());
                }
            }
        }
    }

    @Test
    void nestedLinksDetectCyclesThroughExternalDirectories() throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path external = Files.createDirectory(directory.resolve("external"));
        Files.createSymbolicLink(source.resolve("out"), external);
        Files.createSymbolicLink(external.resolve("back"), source);
        try (UploadManifest manifest = new UploadManifest(directory)) {
            assertThrows(FileSystemLoopException.class, () -> manifest.scan(source));
        }
    }

    @Test
    void linkToContainingDirectoryFailsBeforeTraversingTheAncestorAgain() throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Files.createSymbolicLink(source.resolve("up"), directory);
        try (UploadManifest manifest = new UploadManifest(directory)) {
            FileSystemLoopException failure = assertThrows(FileSystemLoopException.class, () -> manifest.scan(source));
            assertEquals(source.resolve("up").toString(), failure.getFile());
        }
    }

    @Test
    void brokenLinkFailsExplicitly() throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Files.createSymbolicLink(source.resolve("broken"), directory.resolve("missing"));
        try (UploadManifest manifest = new UploadManifest(directory)) {
            assertThrows(NoSuchFileException.class, () -> manifest.scan(source));
        }
    }

    @Test
    void symbolicLinkCycleFailsExplicitly() throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Files.createSymbolicLink(source.resolve("cycle"), source);
        try (UploadManifest manifest = new UploadManifest(directory)) {
            assertThrows(FileSystemLoopException.class, () -> manifest.scan(source));
        }
    }
}
