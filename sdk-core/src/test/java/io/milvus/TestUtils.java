package io.milvus;

import io.milvus.common.utils.Float16Utils;
import io.milvus.grpc.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TestUtils {
    private int dimension = 256;
    private static final Random RANDOM = new Random();

    public static final String MilvusStandaloneUri = "http://localhost:19530";
    private static final String DockerCleanupImageID = "alpine:3.20";
    private static boolean startedMilvusStandalone;
    private static File startedDockerComposeFile;
    private static File startedDockerComposeVolumeDirectory;

    public static File dockerComposeFile(String fileName) {
        File modulePath = new File("src/test/java/io/milvus", fileName);
        if (modulePath.isFile()) {
            return modulePath;
        }
        return new File("sdk-core/src/test/java/io/milvus", fileName);
    }

    public static void startMilvusStandalone(File composeFile, File volumeDirectory, List<String> containerNames) {
        if (!composeFile.isFile()) {
            throw new IllegalStateException("Milvus docker compose file not found: " + composeFile);
        }
        if (containerNames == null || containerNames.isEmpty()) {
            throw new IllegalStateException("Milvus docker compose container names are empty");
        }
        Assumptions.assumeTrue(dockerAvailable(), "Docker is not available");

        volumeDirectory.mkdirs();
        runDockerCompose(composeFile, volumeDirectory, "down", "-v");
        cleanupDockerComposeData(volumeDirectory);
        volumeDirectory.mkdirs();

        runDockerCompose(composeFile, volumeDirectory, "up", "-d");
        try {
            waitForMilvusStandalone(containerNames);
            startedMilvusStandalone = true;
            startedDockerComposeFile = composeFile.getAbsoluteFile();
            startedDockerComposeVolumeDirectory = volumeDirectory.getAbsoluteFile();
        } catch (RuntimeException e) {
            runDockerCompose(composeFile, volumeDirectory, "down", "-v");
            cleanupDockerComposeData(volumeDirectory);
            startedMilvusStandalone = false;
            startedDockerComposeFile = null;
            startedDockerComposeVolumeDirectory = null;
            throw e;
        }
    }

    public static synchronized void stopMilvusStandalone() {
        if (!startedMilvusStandalone) {
            return;
        }

        try {
            runDockerCompose(startedDockerComposeFile, startedDockerComposeVolumeDirectory, "down", "-v");
        } finally {
            cleanupDockerComposeData(startedDockerComposeVolumeDirectory);
            startedMilvusStandalone = false;
            startedDockerComposeFile = null;
            startedDockerComposeVolumeDirectory = null;
        }
    }

    private static boolean dockerAvailable() {
        try {
            return runCommand(Arrays.asList("docker", "info"), new File("."), null).exitCode == 0
                    && runCommand(Arrays.asList("docker", "compose", "version"), new File("."), null).exitCode == 0;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static void runDockerCompose(File composeFile, File volumeDirectory, String... args) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("compose");
        command.add("-f");
        command.add(composeFile.getAbsolutePath());
        Collections.addAll(command, args);

        Map<String, String> environment = new HashMap<>();
        environment.put("DOCKER_VOLUME_DIRECTORY", volumeDirectory.getAbsolutePath());
        CommandResult result = runCommand(command, composeFile.getParentFile(), environment);
        if (result.exitCode != 0) {
            throw new IllegalStateException("Command failed: " + String.join(" ", command) + System.lineSeparator() + result.output);
        }
    }

    private static void waitForMilvusStandalone(List<String> containerNames) {
        long deadline = System.currentTimeMillis() + 300000L;
        while (System.currentTimeMillis() < deadline) {
            if (allContainersHealthy(containerNames)) {
                return;
            }

            for (String containerName : containerNames) {
                String state = inspectContainer("{{.State.Status}}", containerName);
                if ("exited".equals(state) || "dead".equals(state)) {
                    throw new IllegalStateException("Milvus container exited before becoming healthy." + System.lineSeparator()
                            + dockerLogs(containerNames));
                }
            }

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Milvus standalone", e);
            }
        }

        throw new IllegalStateException("Timed out waiting for Milvus standalone to become healthy."
                + System.lineSeparator() + dockerLogs(containerNames));
    }

    private static boolean allContainersHealthy(List<String> containerNames) {
        for (String containerName : containerNames) {
            if (!"healthy".equals(inspectContainer("{{.State.Health.Status}}", containerName))) {
                return false;
            }
        }
        return true;
    }

    private static String inspectContainer(String format, String containerName) {
        CommandResult result = runCommand(Arrays.asList("docker", "inspect", "-f", format, containerName), new File("."), null);
        return result.exitCode == 0 ? result.output.trim() : "";
    }

    private static String dockerLogs(String containerName) {
        return runCommand(Arrays.asList("docker", "logs", containerName), new File("."), null).output;
    }

    private static String dockerLogs(List<String> containerNames) {
        StringBuilder logs = new StringBuilder();
        for (String containerName : containerNames) {
            logs.append("==== ").append(containerName).append(" ====").append(System.lineSeparator());
            logs.append(dockerLogs(containerName));
        }
        return logs.toString();
    }

    private static void cleanupDockerComposeData(File volumeDirectory) {
        if (!volumeDirectory.exists()) {
            return;
        }

        CommandResult result = runCommand(Arrays.asList("docker", "run", "--rm", "--user", "0:0", "--entrypoint", "sh",
                "-v", volumeDirectory.getParentFile().getAbsolutePath() + ":/work", DockerCleanupImageID,
                "-c", "rm -rf /work/" + volumeDirectory.getName()), new File("."), null);
        if (result.exitCode != 0) {
            throw new IllegalStateException("Failed to clean Milvus compose data." + System.lineSeparator() + result.output);
        }
    }

    private static CommandResult runCommand(List<String> command, File workingDirectory, Map<String, String> environment) {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(workingDirectory);
        processBuilder.redirectErrorStream(true);
        if (environment != null) {
            processBuilder.environment().putAll(environment);
        }

        try {
            Process process = processBuilder.start();
            String output = readProcessOutput(process);
            return new CommandResult(process.waitFor(), output);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to run command: " + String.join(" ", command), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while running command: " + String.join(" ", command), e);
        }
    }

    private static String readProcessOutput(Process process) throws IOException {
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }
        }
        return output.toString();
    }

    private static class CommandResult {
        private final int exitCode;
        private final String output;

        private CommandResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
    public TestUtils(int dimension) {
        this.dimension = dimension;
    }

    public List<Float> generateFloatVector(int dim) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dim; ++i) {
            vector.add(RANDOM.nextFloat());
        }
        return vector;
    }

    public List<Float> generateFloatVector() {
        return generateFloatVector(dimension);
    }

    public List<List<Float>> generateFloatVectors(int count) {
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateFloatVector());
        }

        return vectors;
    }

    public ByteBuffer generateBinaryVector(int dim) {
        int byteCount = dim / 8;
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) RANDOM.nextInt(Byte.MAX_VALUE));
        }
        return vector;
    }

    public ByteBuffer generateBinaryVector() {
        return generateBinaryVector(dimension);
    }

    public List<ByteBuffer> generateBinaryVectors(int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateBinaryVector());
        }
        return vectors;

    }

    public ByteBuffer generateFloat16Vector() {
        List<Float> vector = generateFloatVector();
        return Float16Utils.f32VectorToFp16Buffer(vector);
    }

    public List<ByteBuffer> generateFloat16Vectors(int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateFloat16Vector());
        }
        return vectors;
    }

    public ByteBuffer generateBFloat16Vector() {
        List<Float> vector = generateFloatVector();
        return Float16Utils.f32VectorToBf16Buffer(vector);
    }

    public List<ByteBuffer> generateBFloat16Vectors(int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateBFloat16Vector());
        }
        return vectors;
    }

    public SortedMap<Long, Float> generateSparseVector() {
        SortedMap<Long, Float> sparse = new TreeMap<>();
        int dim = RANDOM.nextInt(10) + 10;
        while (sparse.size() < dim) {
            sparse.put((long) RANDOM.nextInt(1000000), RANDOM.nextFloat());
        }
        return sparse;
    }

    public List<SortedMap<Long, Float>> generateSparseVectors(int count) {
        List<SortedMap<Long, Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateSparseVector());
        }
        return vectors;
    }

    public List<?> generateRandomArray(DataType eleType, int maxCapacity) {
        switch (eleType) {
            case Bool: {
                List<Boolean> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(i % 10 == 0);
                }
                return values;
            }
            case Int8:
            case Int16: {
                List<Short> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add((short) RANDOM.nextInt(256));
                }
                return values;
            }
            case Int32: {
                List<Integer> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(RANDOM.nextInt());
                }
                return values;
            }
            case Int64: {
                List<Long> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(RANDOM.nextLong());
                }
                return values;
            }
            case Float: {
                List<Float> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(RANDOM.nextFloat());
                }
                return values;
            }
            case Double: {
                List<Double> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(RANDOM.nextDouble());
                }
                return values;
            }
            case VarChar: {
                List<String> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(String.format("varchar_arr_%d", i));
                }
                return values;
            }
            default:
                Assertions.fail();
        }
        return null;
    }
}
