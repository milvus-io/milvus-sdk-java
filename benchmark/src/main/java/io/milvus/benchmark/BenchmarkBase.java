package io.milvus.benchmark;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.milvus.v2.client.ConnectConfig;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public abstract class BenchmarkBase {

    private static final String DEFAULT_URI = "http://localhost:19530";
    private static final String DEFAULT_TOKEN = "root:Milvus";

    protected String uri;
    protected String token;
    protected String sdkVersion;

    protected abstract String name();

    protected abstract void prepare();

    protected abstract void run();

    protected abstract void printSummary();

    protected abstract void applyConfig(JsonObject config);

    protected ConnectConfig connectConfig() {
        return ConnectConfig.builder().uri(uri).token(token).build();
    }

    public void execute(String[] args) {
        uri = (args.length > 0) ? args[0] : DEFAULT_URI;
        token = (args.length > 1) ? args[1] : DEFAULT_TOKEN;
        sdkVersion = detectSdkVersion();

        System.out.println("=== " + name() + " ===");
        System.out.println("URI: " + uri);
        System.out.println("SDK Version: " + sdkVersion);

        JsonObject config = loadConfig();
        if (config != null) {
            applyConfig(config);
        }
        System.out.println();

        prepare();
        run();
        printSummary();
    }

    protected void writeResultsFile(String content) {
        String className = getClass().getSimpleName();
        String datetime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        File dir = new File("results");
        dir.mkdirs();
        File file = new File(dir, className + "_" + sdkVersion + "_" + datetime + ".md");
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
            System.out.println("Results written to: " + file.getPath());
        } catch (IOException e) {
            System.out.println("Failed to write results file: " + e.getMessage());
        }
    }

    protected String timestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    private static String detectSdkVersion() {
        try (InputStream is = ConnectConfig.class.getResourceAsStream(
                "/META-INF/maven/io.milvus/milvus-sdk-java/pom.properties")) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                return props.getProperty("version", "unknown");
            }
        } catch (IOException e) {
            // ignore
        }
        return "unknown";
    }

    protected JsonObject loadConfig() {
        String className = getClass().getSimpleName();
        File file = new File("config/" + className + ".json");
        if (!file.exists()) {
            return null;
        }
        try (FileReader reader = new FileReader(file)) {
            JsonObject config = JsonParser.parseReader(reader).getAsJsonObject();
            System.out.println("Config: " + file.getPath());
            return config;
        } catch (Exception e) {
            System.out.println("WARNING: Failed to load config file " + file.getPath() + ": " + e.getMessage());
            return null;
        }
    }

    protected static List<Float> generateFloatVector(int dim) {
        Random random = ThreadLocalRandom.current();
        List<Float> vector = new ArrayList<>(dim);
        for (int i = 0; i < dim; i++) {
            vector.add(random.nextFloat());
        }
        return vector;
    }
}
