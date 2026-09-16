package io.milvus.bulkwriter.common.utils;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for processing local file paths.
 *
 * <p>Resolves a local file or directory path into the list of files it contains and their
 * total size, recursing into subdirectories.</p>
 */


public class FileUtils {
    // Get all filePath with the inputFileSuffix in the localPath
    /**
     * Processes the given local file or directory path and returns the files it contains
     * together with their total size.
     *
     * @param localPath the local file or directory path
     * @return a pair containing the list of file paths and their total size
     */

    public static Pair<List<String>, Long> processLocalPath(String localPath) {
        Path path = Paths.get(localPath);
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path does not exist: " + localPath);
        }
        if (Files.isRegularFile(path)) {
            return Pair.of(Lists.newArrayList(path.toString()), path.toFile().length());
        } else if (Files.isDirectory(path)) {
            return FileUtils.findFilesRecursively(path.toFile());
        }
        return Pair.of(new ArrayList<>(), 0L);
    }

    /**
     * Finds files with the given suffix in the first level subdirectories of the folder.
     *
     * @param folder the folder to search recursively
     * @return a pair containing the list of file paths and their total size
     */


    public static Pair<List<String>, Long> findFilesRecursively(File folder) {
        List<String> result = new ArrayList<>();
        long totalSize = 0L;

        File[] entries = folder.listFiles();
        if (entries == null) {
            return Pair.of(result, 0L);
        }

        for (File entry : entries) {
            if (entry.isFile()) {
                result.add(entry.getAbsolutePath());
                totalSize += entry.length();
            } else if (entry.isDirectory()) {
                Pair<List<String>, Long> subResult = findFilesRecursively(entry);
                result.addAll(subResult.getLeft());
                totalSize += subResult.getRight();
            }
        }

        return Pair.of(result, totalSize);
    }
}
