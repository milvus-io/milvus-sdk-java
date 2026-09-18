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
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class UploadManifestScaleTest {
    @TempDir Path directory;

    @Test
    void millionEntriesWith64MiBHeap() throws Exception {
        Path output = directory.resolve("probe.log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx64m", "-cp", classpath, Probe.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(child.waitFor(180, TimeUnit.SECONDS), "Scale probe timed out");
            String log = new String(Files.readAllBytes(output), java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(0, child.exitValue(), log);
            assertTrue(log.contains("PASS entries=1000000"), log);
            System.out.println(log);
        } finally { child.destroyForcibly(); }
    }

    public static class Probe {
        public static void main(String[] args) throws Exception {
            long start = System.nanoTime();
            Path index;
            try (UploadManifest manifest = new UploadManifest(Paths.get(args[0]))) {
                if (manifest.directory() != null) { throw new AssertionError("Index should start in memory"); }
                for (int i = 0; i < 1_000_000; i++) { manifest.add(key(i), 3, 0); }
                manifest.finishScan();
                index = manifest.directory();
                if (index == null) { throw new AssertionError("Large index must spill to disk"); }
                if (manifest.fileCount() != 1_000_000 || manifest.totalBytes() != 3_000_000) {
                    throw new AssertionError("Incorrect scanned totals");
                }
                List<ObjectMetadata> page = new ArrayList<>(1000);
                for (int i = 0; i < 1_000_000; i += 2) {
                    page.add(new ObjectMetadata("remote/" + key(i), 3L, 0L));
                    if (page.size() == 1000) { manifest.excludeExisting(page, "remote/", UploadPolicy.SKIP_IF_SAME_SIZE); page.clear(); }
                }
                if (manifest.remainingCount() != 500_000 || manifest.remainingBytes() != 1_500_000) {
                    throw new AssertionError("Incorrect remaining totals");
                }
                AtomicInteger uploaded = new AtomicInteger();
                AtomicInteger callbacks = new AtomicInteger();
                java.util.concurrent.atomic.AtomicReference<io.milvus.bulkwriter.model.UploadProgress> last =
                        new java.util.concurrent.atomic.AtomicReference<>();
                VolumeFileManager.UploadProgressTracker tracker = new VolumeFileManager.UploadProgressTracker(
                        1_500_000, 500_000, progress -> { callbacks.incrementAndGet(); last.set(progress); });
                try (UploadManifest.Cursor cursor = manifest.openCursor()) {
                    BoundedUploadExecutor.run(4, cursor::next, entry -> {
                        tracker.resetFile(entry.relativePath);
                        tracker.updateFile(entry.relativePath, entry.size, entry.size);
                        tracker.finishFile(entry.relativePath, entry.size);
                        uploaded.incrementAndGet();
                    });
                }
                tracker.finishUpload();
                if (last.get().getCompletedFiles() != 500_000 || last.get().getUploadedBytes() != 1_500_000
                        || last.get().getPercent() != 100.0 || callbacks.get() > 40) {
                    throw new AssertionError("Incorrect or unthrottled progress");
                }
                if (uploaded.get() != 500_000) { throw new AssertionError("Lost upload entries"); }
            }
            if (Files.exists(index)) { throw new AssertionError("Index was not deleted"); }
            System.out.println("PASS entries=1000000 remaining=500000 heapLimit=" + Runtime.getRuntime().maxMemory()
                    + " elapsedSeconds=" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start));
        }

        private static String key(int i) { return "2026/09/17/shard-" + (i % 1000) + "/nested/file-" + i + ".parquet"; }
    }
}
