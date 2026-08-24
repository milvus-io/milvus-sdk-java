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

package io.milvus.telemetry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.milvus.grpc.ClientCommand;
import io.milvus.grpc.ClientHeartbeatRequest;
import io.milvus.grpc.ClientHeartbeatResponse;
import io.milvus.grpc.ClientInfo;
import io.milvus.grpc.ClientTelemetryServiceGrpc;
import io.milvus.grpc.CommandReply;
import io.milvus.grpc.Metrics;
import io.milvus.grpc.OperationMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Collects client operation metrics and exchanges heartbeat commands with Milvus.
 *
 * <p>The manager is thread-safe. Command replies remain queued until a heartbeat succeeds,
 * and persistent command hashes are deterministic across SDK languages.</p>
 */
public final class ClientTelemetryManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClientTelemetryManager.class);
    private static final Gson GSON = new Gson();
    private static final int SAMPLE_BUFFER_SIZE = 1000;
    private static final int SNAPSHOT_LIMIT = 120;
    /**
     * Fixed-point unit for accumulating a fractional sampling rate. A rate becomes an
     * integer step of this many units, so the smallest rate that still samples is 1e-9 --
     * far below anything an operator would set, which is the point: a configured rate must
     * never round down to "off".
     */
    private static final long SAMPLING_SCALE = 1_000_000_000L;
    private static final int MAX_REPLY_BYTES = 1024 * 1024;
    private static final long MAX_UNIMPLEMENTED_BACKOFF_MS = TimeUnit.MINUTES.toMillis(30);
    private static final SecureRandom REQUEST_ID_RANDOM = new SecureRandom();
    private static final List<String> PUSH_CONFIG_KEYS = Arrays.asList(
            "enabled", "heartbeat_interval_ms", "sampling_rate");
    private static final Pattern RFC3339_TIMESTAMP = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})(\\.\\d+)?(Z|[+-]\\d{2}:\\d{2})$");

    private final TelemetryConfig config;
    private final String clientId;
    private final boolean stableClientId;
    private final String user;
    private final String sdkVersion;
    private final Supplier<String> databaseProvider;
    private final Supplier<Map<String, Object>> configProvider;
    private final Map<String, Collector> collectors = new HashMap<>();
    private final Set<String> enabledCollections = new HashSet<>();
    private final Deque<ErrorInfo> errors;
    private final Deque<MetricsSnapshot> snapshots = new ArrayDeque<>();
    private final List<CommandReply> pendingReplies = new ArrayList<>();
    private final Map<String, Long> executedCommands = new HashMap<>();
    private final Map<String, CommandHandler> handlers = new HashMap<>();
    private final Map<String, CommandHandler> customHandlers = new HashMap<>();
    /**
     * Carries the fractional sampling rate between calls, in SAMPLING_SCALE units: each
     * operation adds the rate and the one that pushes it past a whole unit is the one
     * sampled. See shouldSample.
     */
    private final AtomicLong samplingAccum = new AtomicLong();
    private final AtomicBoolean ready = new AtomicBoolean();
    private final AtomicBoolean controlPlaneActivated = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService executor;
    private final Object heartbeatLifecycle = new Object();
    private final Object heartbeatRetirement = new Object();
    private final Object runtimeStateHandoff = new Object();

    private volatile ClientTelemetryServiceGrpc.ClientTelemetryServiceBlockingStub stub;
    private volatile ClientTelemetryManager handoffTarget;
    private volatile boolean allCollectionsEnabled;
    private volatile long lastCommandTimestamp;
    private volatile String configHash = "";
    private volatile int unsupportedStreak;
    private volatile Throwable lastHeartbeatError;
    private volatile long lastSnapshotEnd;
    private long heartbeatGeneration;
    private boolean retirementPending;

    public ClientTelemetryManager(
            TelemetryConfig config,
            String user,
            String sdkVersion,
            Supplier<String> databaseProvider,
            Supplier<Map<String, Object>> configProvider) {
        this(config, user, sdkVersion, databaseProvider, configProvider, "");
    }

    public ClientTelemetryManager(
            TelemetryConfig config,
            String user,
            String sdkVersion,
            Supplier<String> databaseProvider,
            Supplier<Map<String, Object>> configProvider,
            String runtimeClientId) {
        this.config = config == null ? TelemetryConfig.defaults() : config;
        this.stableClientId = this.config.getClientId() != null && !this.config.getClientId().isEmpty();
        this.clientId = stableClientId
                ? this.config.getClientId()
                : (runtimeClientId == null || runtimeClientId.isEmpty()
                        ? UUID.randomUUID().toString() : runtimeClientId);
        this.user = user == null ? "" : user;
        this.sdkVersion = sdkVersion == null ? "" : sdkVersion;
        this.databaseProvider = databaseProvider == null ? () -> "" : databaseProvider;
        this.configProvider = configProvider == null ? HashMap::new : configProvider;
        this.errors = new ArrayDeque<>(this.config.getErrorMaxCount());
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "milvus-telemetry-" + clientId.substring(0, Math.min(8, clientId.length())));
            thread.setDaemon(true);
            return thread;
        });
        registerDefaultHandlers();
    }

    public void setStub(ClientTelemetryServiceGrpc.ClientTelemetryServiceBlockingStub stub) {
        this.stub = stub;
    }

    public void start() {
        if (!ready.compareAndSet(false, true)) {
            return;
        }
        if (!config.isEnabled() && !controlPlaneActivated.get()) {
            return;
        }
        controlPlaneActivated.set(true);
        executor.execute(this::heartbeatAndScheduleNext);
    }

    public boolean isReady() {
        return ready.get();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean isSupported() {
        return unsupportedStreak == 0;
    }

    public Throwable getLastHeartbeatError() {
        return lastHeartbeatError;
    }

    public String getClientId() {
        return clientId;
    }

    /** Returns a non-zero lowercase OpenTelemetry TraceID for client_request_id. */
    public static String newClientRequestId() {
        byte[] value = new byte[16];
        do {
            REQUEST_ID_RANDOM.nextBytes(value);
        } while (allZero(value));
        StringBuilder result = new StringBuilder(32);
        for (byte item : value) {
            result.append(String.format("%02x", item & 0xff));
        }
        return result.toString();
    }

    private static boolean allZero(byte[] value) {
        for (byte item : value) {
            if (item != 0) {
                return false;
            }
        }
        return true;
    }

    public String getConfigHash() {
        return configHash;
    }

    public long getLastCommandTimestamp() {
        return lastCommandTimestamp;
    }

    public TelemetryConfig getConfig() {
        return config;
    }

    public synchronized void registerCommandHandler(String type, CommandHandler handler) {
        handlers.put(type, handler);
        customHandlers.put(type, handler);
    }

    public synchronized RuntimeState snapshotRuntimeState() {
        List<ErrorInfo> errorValues;
        synchronized (errors) {
            errorValues = new ArrayList<>(errors);
        }
        List<MetricsSnapshot> snapshotValues;
        synchronized (snapshots) {
            snapshotValues = new ArrayList<>(snapshots);
        }
        List<CommandReply> replyValues;
        synchronized (pendingReplies) {
            replyValues = new ArrayList<>(pendingReplies);
        }
        Map<String, Long> commandValues;
        synchronized (executedCommands) {
            commandValues = new HashMap<>(executedCommands);
        }
        Set<String> collectionValues;
        boolean allCollections;
        synchronized (enabledCollections) {
            collectionValues = new HashSet<>(enabledCollections);
            allCollections = allCollectionsEnabled;
        }
        Map<String, CommandHandler> handlerValues = new HashMap<>(customHandlers);
        Map<String, Collector> collectorValues = new HashMap<>();
        synchronized (collectors) {
            for (Map.Entry<String, Collector> entry : collectors.entrySet()) {
                collectorValues.put(entry.getKey(), entry.getValue().copy());
            }
        }
        return new RuntimeState(
                clientId,
                configHash,
                lastCommandTimestamp,
                replyValues,
                commandValues,
                collectionValues,
                allCollections,
                handlerValues,
                errorValues,
                snapshotValues,
                collectorValues,
                samplingAccum.get(),
                lastSnapshotEnd,
                unsupportedStreak,
                lastHeartbeatError,
                controlPlaneActivated.get(),
                config.isEnabled(),
                config.getHeartbeatIntervalMs(),
                config.getSamplingRate());
    }

    public synchronized void restoreRuntimeState(RuntimeState state) {
        if (state == null) {
            return;
        }
        if (!clientId.equals(state.clientId)) {
            throw new IllegalArgumentException("telemetry runtime state belongs to a different client ID");
        }
        config.setEnabled(state.enabled);
        config.setHeartbeatIntervalMs(state.heartbeatIntervalMs);
        config.setSamplingRate(state.samplingRate);
        configHash = state.configHash;
        lastCommandTimestamp = state.lastCommandTimestamp;
        samplingAccum.set(state.samplingAccum);
        lastSnapshotEnd = state.lastSnapshotEnd;
        unsupportedStreak = state.unsupportedStreak;
        lastHeartbeatError = state.lastHeartbeatError;
        controlPlaneActivated.set(state.controlPlaneActivated);
        synchronized (pendingReplies) {
            pendingReplies.clear();
            pendingReplies.addAll(state.pendingReplies);
        }
        synchronized (executedCommands) {
            executedCommands.clear();
            executedCommands.putAll(state.executedCommands);
        }
        synchronized (enabledCollections) {
            enabledCollections.clear();
            enabledCollections.addAll(state.enabledCollections);
            allCollectionsEnabled = state.allCollectionsEnabled;
        }
        synchronized (errors) {
            errors.clear();
            errors.addAll(state.errors);
            while (errors.size() > config.getErrorMaxCount()) {
                errors.removeFirst();
            }
        }
        synchronized (snapshots) {
            snapshots.clear();
            snapshots.addAll(state.snapshots);
            while (snapshots.size() > SNAPSHOT_LIMIT) {
                snapshots.removeFirst();
            }
        }
        synchronized (collectors) {
            collectors.clear();
            for (Map.Entry<String, Collector> entry : state.collectors.entrySet()) {
                collectors.put(entry.getKey(), entry.getValue().copy());
            }
        }
        customHandlers.clear();
        customHandlers.putAll(state.customHandlers);
        handlers.putAll(state.customHandlers);
    }

    /** Atomically hands state to a replacement manager after its last heartbeat finishes. */
    public RuntimeState snapshotAndCloseRuntimeState() {
        synchronized (heartbeatLifecycle) {
            RuntimeState state = snapshotRuntimeState();
            close();
            return state;
        }
    }

    /**
     * Invalidates an in-flight heartbeat and prevents another one from starting while the
     * surrounding connection switch is pending. The token can be cancelled if that switch
     * rolls back, without making the old manager unusable.
     */
    public long beginRuntimeStateRetirement() {
        synchronized (heartbeatRetirement) {
            if (retirementPending) {
                throw new IllegalStateException("telemetry retirement is already pending");
            }
            retirementPending = true;
            return ++heartbeatGeneration;
        }
    }

    /** Restores the old manager after the surrounding connection switch rolls back. */
    public void cancelRuntimeStateRetirement(long retirementToken) {
        synchronized (heartbeatRetirement) {
            if (retirementPending && retirementToken == heartbeatGeneration) {
                retirementPending = false;
            }
        }
    }

    /** Completes a state handoff that was fenced before the outer client switch committed. */
    public void handoffRuntimeStateTo(
            ClientTelemetryManager replacement, long retirementToken) {
        if (replacement == null || replacement == this) {
            throw new IllegalArgumentException("replacement telemetry manager is required");
        }
        if (replacement.isReady()) {
            throw new IllegalStateException("replacement telemetry worker must be deferred");
        }
        synchronized (heartbeatRetirement) {
            if (!retirementPending || retirementToken != heartbeatGeneration) {
                throw new IllegalStateException("telemetry retirement token is not active");
            }
        }
        synchronized (heartbeatLifecycle) {
            synchronized (runtimeStateHandoff) {
                if (closed.get()) {
                    throw new IllegalStateException("telemetry manager is already closed");
                }
                synchronized (replacement.runtimeStateHandoff) {
                    if (replacement.handoffTarget != this) {
                        throw new IllegalStateException("replacement telemetry manager was not prepared");
                    }
                    replacement.restoreRuntimeState(snapshotRuntimeState());
                    replacement.handoffTarget = null;
                    handoffTarget = replacement;
                    close();
                }
            }
        }
        replacement.start();
    }

    /** Redirects replacement-client operations to the active manager until the switch commits. */
    public void prepareRuntimeStateHandoffFrom(ClientTelemetryManager activeManager) {
        if (activeManager == null || activeManager == this) {
            throw new IllegalArgumentException("active telemetry manager is required");
        }
        if (isReady() || isClosed()) {
            throw new IllegalStateException("replacement telemetry worker must be deferred and open");
        }
        synchronized (runtimeStateHandoff) {
            handoffTarget = activeManager;
        }
    }

    /** Cancels a prepared handoff after the surrounding client switch failed. */
    public void cancelRuntimeStateHandoffFrom(ClientTelemetryManager activeManager) {
        synchronized (runtimeStateHandoff) {
            if (handoffTarget == activeManager) {
                handoffTarget = null;
            }
        }
    }

    public void recordOperation(
            String operation, String collection, long startNanos, String error, String requestId) {
        ClientTelemetryManager target;
        synchronized (runtimeStateHandoff) {
            target = handoffTarget;
            if (target == null) {
                recordOperationLocally(operation, collection, startNanos, error, requestId);
            }
        }
        if (target != null) {
            target.recordOperation(operation, collection, startNanos, error, requestId);
        }
    }

    private void recordOperationLocally(
            String operation, String collection, long startNanos, String error, String requestId) {
        if (!config.isEnabled() || !shouldSample(config.getSamplingRate())) {
            return;
        }
        long latencyMicros = Math.max(0, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos));
        String collectionKey;
        synchronized (enabledCollections) {
            collectionKey = allCollectionsEnabled || enabledCollections.contains(collection) ? collection : "";
        }
        synchronized (collectors) {
            collectors.computeIfAbsent(operation, ignored -> new Collector())
                    .record(collectionKey, latencyMicros, error == null || error.isEmpty());
        }
        if (error != null && !error.isEmpty()) {
            synchronized (errors) {
                while (errors.size() >= config.getErrorMaxCount()) {
                    errors.removeFirst();
                }
                errors.addLast(new ErrorInfo(
                        System.currentTimeMillis(), operation, error,
                        collection == null ? "" : collection, requestId == null ? "" : requestId));
            }
        }
    }

    public List<ErrorInfo> getRecentErrors(int maxCount) {
        List<ErrorInfo> result;
        synchronized (errors) {
            result = new ArrayList<>(errors);
        }
        Collections.reverse(result);
        if (maxCount >= 0 && result.size() > maxCount) {
            return new ArrayList<>(result.subList(0, maxCount));
        }
        return result;
    }

    public List<MetricsSnapshot> getMetricsSnapshots() {
        synchronized (snapshots) {
            return new ArrayList<>(snapshots);
        }
    }

    public synchronized void processCommands(List<ClientCommand> commands) {
        long previousTimestamp = lastCommandTimestamp;
        long maxTimestamp = previousTimestamp;
        boolean hasPersistent = false;
        for (ClientCommand command : commands) {
            maxTimestamp = Math.max(maxTimestamp, command.getCreateTime());
            hasPersistent |= command.getPersistent();
            if (command.getCreateTime() < previousTimestamp) {
                queueReply(successReply(command.getCommandId(), ByteString.EMPTY));
                continue;
            }
            synchronized (executedCommands) {
                if (executedCommands.containsKey(command.getCommandId())) {
                    queueReply(successReply(command.getCommandId(), ByteString.EMPTY));
                    continue;
                }
            }
            CommandReply reply = handleCommand(command);
            synchronized (executedCommands) {
                executedCommands.put(command.getCommandId(), command.getCreateTime());
            }
            if (reply != null) {
                queueReply(reply);
            }
        }
        long cursorTimestamp = maxTimestamp;
        synchronized (executedCommands) {
            // Keep every ID at the current cursor. The server may resend a batch with the same
            // timestamp more than once; retaining those IDs prevents the third delivery from
            // executing after the second delivery's cleanup.
            executedCommands.entrySet().removeIf(entry -> entry.getValue() < cursorTimestamp);
        }
        if (hasPersistent) {
            configHash = calculateConfigHash(commands);
        }
        lastCommandTimestamp = Math.max(lastCommandTimestamp, maxTimestamp);
    }

    public static String calculateConfigHash(List<ClientCommand> commands) {
        List<ClientCommand> persistent = new ArrayList<>();
        for (ClientCommand command : commands) {
            if (command.getPersistent()) {
                persistent.add(command);
            }
        }
        if (persistent.isEmpty()) {
            return "";
        }
        persistent.sort(Comparator.comparing(ClientCommand::getCommandId));
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (ClientCommand command : persistent) {
                digest.update(command.getCommandId().getBytes(StandardCharsets.UTF_8));
                digest.update(command.getCommandType().getBytes(StandardCharsets.UTF_8));
                digest.update(command.getPayload().toByteArray());
            }
            byte[] hash = digest.digest();
            StringBuilder result = new StringBuilder(16);
            for (int index = 0; index < 8; index++) {
                result.append(String.format("%02x", hash[index]));
            }
            return result.toString();
        } catch (Exception exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private void heartbeatAndScheduleNext() {
        synchronized (heartbeatLifecycle) {
            if (closed.get()) {
                return;
            }
            createSnapshot();
            sendHeartbeat();
            if (!closed.get()) {
                executor.schedule(this::heartbeatAndScheduleNext, nextDelayMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private long nextDelayMs() {
        long interval = Math.max(1, config.getHeartbeatIntervalMs());
        if (unsupportedStreak <= 0) {
            return interval;
        }
        int exponent = Math.min(unsupportedStreak, 30);
        long multiplier = 1L << Math.min(exponent, 20);
        long delay = interval > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : interval * multiplier;
        return Math.max(interval, Math.min(MAX_UNIMPLEMENTED_BACKOFF_MS, delay));
    }

    private void sendHeartbeat() {
        ClientTelemetryServiceGrpc.ClientTelemetryServiceBlockingStub currentStub;
        long requestGeneration;
        boolean metricsEnabled;
        synchronized (heartbeatRetirement) {
            if (retirementPending) {
                return;
            }
            currentStub = stub;
            requestGeneration = heartbeatGeneration;
            metricsEnabled = config.isEnabled();
            if (currentStub == null) {
                return;
            }
        }
        MetricsSnapshot latest;
        synchronized (snapshots) {
            latest = metricsEnabled ? snapshots.peekLast() : null;
        }
        List<CommandReply> replies;
        synchronized (pendingReplies) {
            replies = new ArrayList<>(pendingReplies);
        }
        ClientHeartbeatRequest request = ClientHeartbeatRequest.newBuilder()
                .setClientInfo(buildClientInfo())
                .setReportTimestamp(System.currentTimeMillis())
                .addAllMetrics(toProtoMetrics(latest == null ? Collections.emptyList() : latest.metrics))
                .addAllCommandReplies(replies)
                .setConfigHash(configHash)
                .setLastCommandTimestamp(lastCommandTimestamp)
                .build();
        try {
            ClientHeartbeatResponse response = currentStub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .clientHeartbeat(request);
            synchronized (heartbeatRetirement) {
                if (retirementPending || requestGeneration != heartbeatGeneration) {
                    return;
                }
                // Any real response proves the server implements telemetry. Business errors must
                // not preserve an earlier UNIMPLEMENTED backoff.
                unsupportedStreak = 0;
                if (response.getStatus().getCode() != 0
                        || response.getStatus().getErrorCodeValue() != 0) {
                    lastHeartbeatError = new IllegalStateException(
                            response.getStatus().getReason());
                    return;
                }
                lastHeartbeatError = null;
                synchronized (pendingReplies) {
                    int sent = Math.min(replies.size(), pendingReplies.size());
                    pendingReplies.subList(0, sent).clear();
                }
                processCommands(response.getCommandsList());
            }
        } catch (StatusRuntimeException exception) {
            synchronized (heartbeatRetirement) {
                if (retirementPending || requestGeneration != heartbeatGeneration) {
                    return;
                }
                lastHeartbeatError = exception;
                if (exception.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
                    unsupportedStreak++;
                }
            }
        } catch (RuntimeException exception) {
            synchronized (heartbeatRetirement) {
                if (retirementPending || requestGeneration != heartbeatGeneration) {
                    return;
                }
                lastHeartbeatError = exception;
                logger.debug("Client telemetry heartbeat failed", exception);
            }
        }
    }

    private ClientInfo buildClientInfo() {
        Map<String, String> reserved = new HashMap<>();
        reserved.put("client_id", clientId);
        reserved.put("client_id_stable", String.valueOf(stableClientId));
        String database = databaseProvider.get();
        if (database != null && !database.isEmpty()) {
            reserved.put("db_name", database);
        }
        String host = "Unknown";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) {
            // Best effort metadata.
        }
        return ClientInfo.newBuilder()
                .setSdkType("Java")
                .setSdkVersion(sdkVersion)
                .setUser(user)
                .setHost(host)
                .setLocalTime(OffsetDateTime.now().toString())
                .putAllReserved(reserved)
                .build();
    }

    /**
     * Decides whether this operation is recorded, spreading the sampled ones evenly rather
     * than in runs.
     *
     * <p>Each call adds the rate to a shared accumulator and samples on the call that
     * carries it across a whole unit: at 0.25 that is every fourth operation. What matters
     * is that the ratio holds over any stretch of calls, not only over a long one --
     * metrics are reported per heartbeat window, and a window is tens or hundreds of
     * operations. A scheme that sampled a contiguous run and then dropped one would give
     * the right long-run ratio while making every individual window either complete or
     * empty.
     *
     * <p>The accumulator is shared, so concurrent callers reorder which of them observes a
     * crossing, but each crossing is observed exactly once: addAndGet hands every caller a
     * distinct interval and the step is smaller than one unit, so no interval spans two
     * crossings.
     */
    private boolean shouldSample(double rate) {
        if (rate >= 1.0) {
            return true;
        }
        if (rate <= 0.0) {
            return false;
        }
        // A rate too small to represent still means "sample rarely", never "sample never":
        // silently disabling telemetry for a positive rate is the one outcome nobody could
        // have intended.
        long step = Math.max(1L, (long) (rate * SAMPLING_SCALE));
        long after = samplingAccum.addAndGet(step);
        long before = after - step;
        return after / SAMPLING_SCALE != before / SAMPLING_SCALE;
    }

    private void createSnapshot() {
        if (!config.isEnabled()) {
            return;
        }
        CollectionScope collectionScope = snapshotCollectionScope();
        List<OperationSnapshot> metrics = new ArrayList<>();
        synchronized (collectors) {
            for (Map.Entry<String, Collector> entry : collectors.entrySet()) {
                OperationSnapshot snapshot = entry.getValue().snapshot(entry.getKey());
                if (snapshot != null) {
                    metrics.add(filterCollectionMetrics(snapshot, collectionScope));
                }
            }
        }
        long now = System.currentTimeMillis();
        long start = lastSnapshotEnd == 0 || lastSnapshotEnd > now
                ? now - config.getHeartbeatIntervalMs() : lastSnapshotEnd;
        lastSnapshotEnd = now;
        synchronized (snapshots) {
            while (snapshots.size() >= SNAPSHOT_LIMIT) {
                snapshots.removeFirst();
            }
            snapshots.addLast(new MetricsSnapshot(start, now, metrics));
        }
    }

    private List<OperationMetrics> toProtoMetrics(List<OperationSnapshot> snapshots) {
        CollectionScope collectionScope = snapshotCollectionScope();
        List<OperationMetrics> result = new ArrayList<>();
        for (OperationSnapshot operation : snapshots) {
            OperationMetrics.Builder builder = OperationMetrics.newBuilder()
                    .setOperation(operation.operation)
                    .setGlobal(toProtoMetric(operation.global));
            for (Map.Entry<String, MetricSnapshot> entry : operation.collections.entrySet()) {
                if (collectionScope.includes(entry.getKey())) {
                    builder.putCollectionMetrics(entry.getKey(), toProtoMetric(entry.getValue()));
                }
            }
            result.add(builder.build());
        }
        return result;
    }

    private CollectionScope snapshotCollectionScope() {
        synchronized (enabledCollections) {
            return new CollectionScope(allCollectionsEnabled, new HashSet<>(enabledCollections));
        }
    }

    private static OperationSnapshot filterCollectionMetrics(
            OperationSnapshot operation, CollectionScope collectionScope) {
        Map<String, MetricSnapshot> collections = new LinkedHashMap<>();
        for (Map.Entry<String, MetricSnapshot> entry : operation.collections.entrySet()) {
            if (collectionScope.includes(entry.getKey())) {
                collections.put(entry.getKey(), entry.getValue());
            }
        }
        return new OperationSnapshot(operation.operation, operation.global, collections);
    }

    private static Metrics toProtoMetric(MetricSnapshot metric) {
        return Metrics.newBuilder()
                .setRequestCount(metric.requestCount)
                .setSuccessCount(metric.successCount)
                .setErrorCount(metric.errorCount)
                .setAvgLatencyMs(metric.avgLatencyMs)
                .setP99LatencyMs(metric.p99LatencyMs)
                .setMaxLatencyMs(metric.maxLatencyMs)
                .build();
    }

    private synchronized CommandReply handleCommand(ClientCommand command) {
        CommandHandler handler = handlers.get(command.getCommandType());
        if (handler == null) {
            return failedReply(command.getCommandId(), "unknown command type: " + command.getCommandType());
        }
        try {
            return handler.handle(command);
        } catch (Exception exception) {
            return failedReply(command.getCommandId(), exception.getMessage());
        }
    }

    private void queueReply(CommandReply reply) {
        synchronized (pendingReplies) {
            pendingReplies.add(reply);
        }
    }

    private void registerDefaultHandlers() {
        registerDefaultCommandHandler("push_config", this::handlePushConfig);
        registerDefaultCommandHandler("collection_metrics", this::handleCollectionMetrics);
        registerDefaultCommandHandler("show_errors", this::handleShowErrors);
        registerDefaultCommandHandler("show_latency_history", this::handleLatencyHistory);
        registerDefaultCommandHandler("get_config", this::handleGetConfig);
    }

    private synchronized void registerDefaultCommandHandler(String type, CommandHandler handler) {
        handlers.put(type, handler);
    }

    private CommandReply handlePushConfig(ClientCommand command) {
        JsonObject payload = payload(command);
        Boolean enabled = optionalBoolean(payload, "enabled");
        Long heartbeatIntervalMs = optionalLong(payload, "heartbeat_interval_ms");
        Double samplingRate = optionalDouble(payload, "sampling_rate");
        // Go decodes ttl_seconds into an int64 before deciding that Java does not apply it.
        // Preserve that typed-payload validation while still reporting the field as ignored.
        optionalLong(payload, "ttl_seconds");

        if (heartbeatIntervalMs != null && heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeat_interval_ms must be positive");
        }

        // No mutation occurs before every supplied value is known to be valid.
        synchronized (config) {
            if (enabled != null) {
                config.setEnabled(enabled);
            }
            if (heartbeatIntervalMs != null) {
                config.setHeartbeatIntervalMs(heartbeatIntervalMs);
            }
            if (samplingRate != null) {
                config.setSamplingRate(samplingRate);
            }
        }

        JsonArray applied = new JsonArray();
        for (String key : PUSH_CONFIG_KEYS) {
            if (payload.has(key) && !payload.get(key).isJsonNull()) {
                applied.add(key);
            }
        }
        List<String> ignoredKeys = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : payload.entrySet()) {
            if (!PUSH_CONFIG_KEYS.contains(entry.getKey())) {
                ignoredKeys.add(entry.getKey());
            }
        }
        Collections.sort(ignoredKeys);
        JsonObject result = new JsonObject();
        result.add("applied", applied);
        if (!ignoredKeys.isEmpty()) {
            JsonArray ignored = new JsonArray();
            for (String key : ignoredKeys) {
                ignored.add(key);
            }
            result.add("ignored", ignored);
        }
        return successReply(command.getCommandId(), bytes(result));
    }

    private CommandReply handleCollectionMetrics(ClientCommand command) {
        if (command.getPayload().isEmpty()) {
            JsonObject result = new JsonObject();
            JsonArray names = new JsonArray();
            synchronized (enabledCollections) {
                List<String> sorted = new ArrayList<>(enabledCollections);
                Collections.sort(sorted);
                for (String name : sorted) {
                    names.add(name);
                }
                result.add("enabled_collections", names);
                result.addProperty("all_collections_enabled", allCollectionsEnabled);
            }
            return successReply(command.getCommandId(), bytes(result));
        }
        JsonObject payload = payload(command);
        boolean enabled = Boolean.TRUE.equals(optionalBoolean(payload, "enabled"));
        List<String> collections = optionalStringArray(payload, "collections");
        // The server currently treats metric types as advisory, but Go still strongly types
        // the field while decoding the command payload.
        optionalStringArray(payload, "metrics_types");
        boolean wildcard = collections.contains("*");
        synchronized (enabledCollections) {
            if (enabled) {
                if (collections.isEmpty()) {
                    throw new IllegalArgumentException("collections list cannot be empty when enabled=true");
                }
                if (wildcard) {
                    allCollectionsEnabled = true;
                } else {
                    enabledCollections.addAll(collections);
                }
            } else if (wildcard || collections.isEmpty()) {
                allCollectionsEnabled = false;
                enabledCollections.clear();
            } else {
                enabledCollections.removeAll(collections);
            }
        }
        return successReply(command.getCommandId(), ByteString.EMPTY);
    }

    private CommandReply handleShowErrors(ClientCommand command) {
        JsonObject payload = payload(command);
        Integer suppliedMaxCount = optionalInt(payload, "max_count");
        int maxCount = suppliedMaxCount == null ? 100 : suppliedMaxCount;
        if (maxCount <= 0) {
            maxCount = 100;
        }
        List<ErrorInfo> recent = getRecentErrors(maxCount);
        if (recent.isEmpty()) {
            return successReply(command.getCommandId(), ByteString.EMPTY);
        }
        byte[] encoded = GSON.toJson(recent).getBytes(StandardCharsets.UTF_8);
        while (encoded.length > MAX_REPLY_BYTES && recent.size() > 1) {
            recent = new ArrayList<>(recent.subList(0, Math.max(1, recent.size() / 2)));
            encoded = GSON.toJson(recent).getBytes(StandardCharsets.UTF_8);
        }
        while (encoded.length > MAX_REPLY_BYTES && recent.size() == 1
                && recent.get(0).error_msg.length() > 1) {
            ErrorInfo current = recent.get(0);
            String message = current.error_msg.substring(0, Math.max(1, current.error_msg.length() / 2))
                    + "...(truncated)";
            recent.set(0, new ErrorInfo(current.timestamp, current.operation, message,
                    current.collection, current.request_id));
            encoded = GSON.toJson(recent).getBytes(StandardCharsets.UTF_8);
        }
        if (encoded.length > MAX_REPLY_BYTES) {
            return failedReply(command.getCommandId(), "show_errors response exceeds the 1MB payload limit");
        }
        return successReply(command.getCommandId(), ByteString.copyFrom(encoded));
    }

    private CommandReply handleGetConfig(ClientCommand command) {
        Map<String, Object> userConfig = new LinkedHashMap<>();
        Map<String, Object> supplied = configProvider.get();
        if (supplied != null) {
            userConfig.putAll(supplied);
        }
        for (String key : Arrays.asList("password", "token", "api_key", "authorization")) {
            userConfig.remove(key);
        }
        userConfig.put("telemetry_enabled", config.isEnabled());
        userConfig.put("telemetry_heartbeat_interval_ms", config.getHeartbeatIntervalMs());
        userConfig.put("telemetry_sampling_rate", config.getSamplingRate());
        synchronized (enabledCollections) {
            List<String> collections = allCollectionsEnabled
                    ? Collections.singletonList("*") : new ArrayList<>(enabledCollections);
            Collections.sort(collections);
            userConfig.put("enabled_collections", collections);
            userConfig.put("all_collections_enabled", allCollectionsEnabled);
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("user_config", userConfig);
        return successReply(command.getCommandId(), ByteString.copyFromUtf8(GSON.toJson(response)));
    }

    private CommandReply handleLatencyHistory(ClientCommand command) {
        JsonObject payload = payload(command);
        String startTime = optionalString(payload, "start_time");
        String endTime = optionalString(payload, "end_time");
        Boolean detail = optionalBoolean(payload, "detail");
        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("payload is required with start_time and end_time");
        }
        long start = parseTimestamp(startTime);
        long end = parseTimestamp(endTime);
        if (end < start) {
            throw new IllegalArgumentException("end_time must be after start_time");
        }
        if (end - start > TimeUnit.HOURS.toMillis(1)) {
            throw new IllegalArgumentException("time range cannot exceed 1 hour");
        }
        List<MetricsSnapshot> matching = new ArrayList<>();
        for (MetricsSnapshot snapshot : getMetricsSnapshots()) {
            if (snapshot.endTime >= start && snapshot.timestamp <= end) {
                matching.add(snapshot);
            }
        }
        Object response = Boolean.TRUE.equals(detail)
                ? detailHistory(matching) : aggregateHistory(matching, start, end);
        byte[] encoded = GSON.toJson(response).getBytes(StandardCharsets.UTF_8);
        if (encoded.length > MAX_REPLY_BYTES) {
            throw new IllegalArgumentException("response too large, try a smaller time range");
        }
        return successReply(command.getCommandId(), ByteString.copyFrom(encoded));
    }

    private static Map<String, Object> detailHistory(List<MetricsSnapshot> snapshots) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> details = new ArrayList<>();
        for (MetricsSnapshot snapshot : snapshots) {
            Map<String, Object> metrics = new LinkedHashMap<>();
            for (OperationSnapshot operation : snapshot.metrics) {
                metrics.put(operation.operation, metricHistory(operation.global));
            }
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("timestamp", snapshot.timestamp);
            detail.put("end_time", snapshot.end_time);
            detail.put("metrics", metrics);
            details.add(detail);
        }
        result.put("snapshots", details);
        result.put("total_snapshots", snapshots.size());
        return result;
    }

    private static Map<String, Object> metricHistory(MetricSnapshot value) {
        Map<String, Object> metric = new LinkedHashMap<>();
        metric.put("request_count", value.requestCount);
        metric.put("success_count", value.successCount);
        metric.put("error_count", value.errorCount);
        metric.put("avg_latency_ms", value.avgLatencyMs);
        metric.put("p99_latency_ms", value.p99LatencyMs);
        metric.put("max_latency_ms", value.maxLatencyMs);
        return metric;
    }

    private static Map<String, Object> aggregateHistory(
            List<MetricsSnapshot> snapshots, long start, long end) {
        Map<String, double[]> totals = new LinkedHashMap<>();
        for (MetricsSnapshot snapshot : snapshots) {
            for (OperationSnapshot operation : snapshot.metrics) {
                MetricSnapshot metric = operation.global;
                double[] total = totals.computeIfAbsent(operation.operation, ignored -> new double[6]);
                total[0] += metric.requestCount;
                total[1] += metric.successCount;
                total[2] += metric.errorCount;
                total[3] += metric.avgLatencyMs * metric.requestCount;
                total[4] += metric.p99LatencyMs * metric.requestCount;
                total[5] = Math.max(total[5], metric.maxLatencyMs);
            }
        }
        Map<String, Object> metrics = new LinkedHashMap<>();
        for (Map.Entry<String, double[]> entry : totals.entrySet()) {
            double[] total = entry.getValue();
            long count = (long) total[0];
            Map<String, Object> metric = new LinkedHashMap<>();
            metric.put("request_count", count);
            metric.put("success_count", (long) total[1]);
            metric.put("error_count", (long) total[2]);
            metric.put("avg_latency_ms", count == 0 ? 0.0 : total[3] / count);
            metric.put("p99_latency_ms", count == 0 ? 0.0 : total[4] / count);
            metric.put("max_latency_ms", total[5]);
            metrics.put(entry.getKey(), metric);
        }
        Map<String, Object> aggregated = new LinkedHashMap<>();
        aggregated.put("start_time", start);
        aggregated.put("end_time", end);
        aggregated.put("metrics", metrics);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("aggregated", aggregated);
        response.put("snapshot_count", snapshots.size());
        return response;
    }

    private static long parseTimestamp(String value) {
        Matcher matcher = RFC3339_TIMESTAMP.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("timestamp must use RFC3339 with seconds and timezone");
        }
        String fraction = matcher.group(2);
        // OffsetDateTime stores nanoseconds while Go accepts a longer RFC3339 secfrac and
        // ultimately reports milliseconds. Truncating beyond nine digits preserves that result.
        if (fraction != null && fraction.length() > 10) {
            fraction = fraction.substring(0, 10);
        }
        String normalized = matcher.group(1) + (fraction == null ? "" : fraction) + matcher.group(3);
        return OffsetDateTime.parse(normalized).toInstant().toEpochMilli();
    }

    private static JsonObject payload(ClientCommand command) {
        if (command.getPayload().isEmpty()) {
            return new JsonObject();
        }
        JsonElement decoded = JsonParser.parseString(command.getPayload().toStringUtf8());
        if (!decoded.isJsonObject()) {
            throw new IllegalArgumentException("payload must be a JSON object");
        }
        return decoded.getAsJsonObject();
    }

    private static Boolean optionalBoolean(JsonObject payload, String name) {
        JsonElement value = payload.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isBoolean()) {
            throw new IllegalArgumentException(name + " must be a boolean");
        }
        return value.getAsBoolean();
    }

    private static String optionalString(JsonObject payload, String name) {
        JsonElement value = payload.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new IllegalArgumentException(name + " must be a string");
        }
        return value.getAsString();
    }

    private static Long optionalLong(JsonObject payload, String name) {
        JsonElement value = payload.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
        String encoded = value.getAsString();
        if (!encoded.matches("-?(0|[1-9][0-9]*)")) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
        try {
            return Long.parseLong(encoded);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " is out of range", exception);
        }
    }

    private static Integer optionalInt(JsonObject payload, String name) {
        Long value = optionalLong(payload, name);
        if (value == null) {
            return null;
        }
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " is out of range");
        }
        return value.intValue();
    }

    private static Double optionalDouble(JsonObject payload, String name) {
        JsonElement value = payload.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw new IllegalArgumentException(name + " must be a number");
        }
        double decoded = value.getAsDouble();
        if (!Double.isFinite(decoded)) {
            throw new IllegalArgumentException(name + " is out of range");
        }
        return decoded;
    }

    private static List<String> optionalStringArray(JsonObject payload, String name) {
        JsonElement value = payload.get(name);
        if (value == null || value.isJsonNull()) {
            return new ArrayList<>();
        }
        if (!value.isJsonArray()) {
            throw new IllegalArgumentException(name + " must be an array of strings");
        }
        List<String> decoded = new ArrayList<>();
        for (JsonElement item : value.getAsJsonArray()) {
            if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isString()) {
                throw new IllegalArgumentException(name + " must be an array of strings");
            }
            decoded.add(item.getAsString());
        }
        return decoded;
    }

    private static ByteString bytes(JsonObject value) {
        return ByteString.copyFromUtf8(GSON.toJson(value));
    }

    private static CommandReply successReply(String commandId, ByteString payload) {
        return CommandReply.newBuilder()
                .setCommandId(commandId)
                .setSuccess(true)
                .setPayload(payload)
                .build();
    }

    private static CommandReply failedReply(String commandId, String error) {
        return CommandReply.newBuilder()
                .setCommandId(commandId)
                .setSuccess(false)
                .setErrorMessage(error == null ? "command failed" : error)
                .build();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            executor.shutdownNow();
        }
    }

    @FunctionalInterface
    public interface CommandHandler {
        CommandReply handle(ClientCommand command) throws Exception;
    }

    /** In-memory telemetry and command state transferred to a replacement connection manager. */
    public static final class RuntimeState {
        private final String clientId;
        private final String configHash;
        private final long lastCommandTimestamp;
        private final List<CommandReply> pendingReplies;
        private final Map<String, Long> executedCommands;
        private final Set<String> enabledCollections;
        private final boolean allCollectionsEnabled;
        private final Map<String, CommandHandler> customHandlers;
        private final List<ErrorInfo> errors;
        private final List<MetricsSnapshot> snapshots;
        private final Map<String, Collector> collectors;
        private final long samplingAccum;
        private final long lastSnapshotEnd;
        private final int unsupportedStreak;
        private final Throwable lastHeartbeatError;
        private final boolean controlPlaneActivated;
        private final boolean enabled;
        private final long heartbeatIntervalMs;
        private final double samplingRate;

        private RuntimeState(
                String clientId,
                String configHash,
                long lastCommandTimestamp,
                List<CommandReply> pendingReplies,
                Map<String, Long> executedCommands,
                Set<String> enabledCollections,
                boolean allCollectionsEnabled,
                Map<String, CommandHandler> customHandlers,
                List<ErrorInfo> errors,
                List<MetricsSnapshot> snapshots,
                Map<String, Collector> collectors,
                long samplingAccum,
                long lastSnapshotEnd,
                int unsupportedStreak,
                Throwable lastHeartbeatError,
                boolean controlPlaneActivated,
                boolean enabled,
                long heartbeatIntervalMs,
                double samplingRate) {
            this.clientId = clientId;
            this.configHash = configHash;
            this.lastCommandTimestamp = lastCommandTimestamp;
            this.pendingReplies = new ArrayList<>(pendingReplies);
            this.executedCommands = new HashMap<>(executedCommands);
            this.enabledCollections = new HashSet<>(enabledCollections);
            this.allCollectionsEnabled = allCollectionsEnabled;
            this.customHandlers = new HashMap<>(customHandlers);
            this.errors = new ArrayList<>(errors);
            this.snapshots = new ArrayList<>(snapshots);
            this.collectors = new HashMap<>();
            for (Map.Entry<String, Collector> entry : collectors.entrySet()) {
                this.collectors.put(entry.getKey(), entry.getValue().copy());
            }
            this.samplingAccum = samplingAccum;
            this.lastSnapshotEnd = lastSnapshotEnd;
            this.unsupportedStreak = unsupportedStreak;
            this.lastHeartbeatError = lastHeartbeatError;
            this.controlPlaneActivated = controlPlaneActivated;
            this.enabled = enabled;
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.samplingRate = samplingRate;
        }

        public String getClientId() {
            return clientId;
        }

        public int getPendingReplyCount() {
            return pendingReplies.size();
        }
    }

    public static final class ErrorInfo {
        public final long timestamp;
        public final String operation;
        public final String error_msg;
        public final String collection;
        public final String request_id;

        ErrorInfo(long timestamp, String operation, String error, String collection, String requestId) {
            this.timestamp = timestamp;
            this.operation = operation;
            this.error_msg = error;
            this.collection = collection;
            this.request_id = requestId;
        }
    }

    public static final class MetricsSnapshot {
        public final long timestamp;
        public final long end_time;
        public final List<OperationSnapshot> metrics;
        private final long endTime;

        MetricsSnapshot(long timestamp, long endTime, List<OperationSnapshot> metrics) {
            this.timestamp = timestamp;
            this.end_time = endTime;
            this.endTime = endTime;
            this.metrics = metrics;
        }
    }

    public static final class OperationSnapshot {
        public final String operation;
        public final MetricSnapshot global;
        public final Map<String, MetricSnapshot> collection_metrics;
        private final Map<String, MetricSnapshot> collections;

        OperationSnapshot(String operation, MetricSnapshot global, Map<String, MetricSnapshot> collections) {
            this.operation = operation;
            this.global = global;
            this.collection_metrics = collections;
            this.collections = collections;
        }
    }

    public static final class MetricSnapshot {
        public final long request_count;
        public final long success_count;
        public final long error_count;
        public final double avg_latency_ms;
        public final double p99_latency_ms;
        public final double max_latency_ms;
        private final long requestCount;
        private final long successCount;
        private final long errorCount;
        private final double avgLatencyMs;
        private final double p99LatencyMs;
        private final double maxLatencyMs;

        MetricSnapshot(long requests, long successes, long failures, double average, double p99, double max) {
            this.request_count = requests;
            this.success_count = successes;
            this.error_count = failures;
            this.avg_latency_ms = average;
            this.p99_latency_ms = p99;
            this.max_latency_ms = max;
            this.requestCount = requests;
            this.successCount = successes;
            this.errorCount = failures;
            this.avgLatencyMs = average;
            this.p99LatencyMs = p99;
            this.maxLatencyMs = max;
        }
    }

    private static final class Collector {
        private MetricBucket global = new MetricBucket();
        private final Map<String, MetricBucket> collections = new HashMap<>();

        Collector copy() {
            Collector result = new Collector();
            result.global = global.copy();
            for (Map.Entry<String, MetricBucket> entry : collections.entrySet()) {
                result.collections.put(entry.getKey(), entry.getValue().copy());
            }
            return result;
        }

        void record(String collection, long latencyMicros, boolean success) {
            global.record(latencyMicros, success);
            if (collection != null && !collection.isEmpty()) {
                collections.computeIfAbsent(collection, ignored -> new MetricBucket())
                        .record(latencyMicros, success);
            }
        }

        OperationSnapshot snapshot(String operation) {
            MetricSnapshot globalSnapshot = global.snapshot();
            if (globalSnapshot == null) {
                return null;
            }
            Map<String, MetricSnapshot> collectionSnapshots = new LinkedHashMap<>();
            for (Map.Entry<String, MetricBucket> entry : collections.entrySet()) {
                MetricSnapshot snapshot = entry.getValue().snapshot();
                if (snapshot != null) {
                    collectionSnapshots.put(entry.getKey(), snapshot);
                }
            }
            global = new MetricBucket();
            collections.clear();
            return new OperationSnapshot(operation, globalSnapshot, collectionSnapshots);
        }
    }

    private static final class MetricBucket {
        private long requests;
        private long successes;
        private long failures;
        private long totalLatencyMicros;
        private long maxLatencyMicros;
        private final long[] samples = new long[SAMPLE_BUFFER_SIZE];
        private int sampleCount;
        private int sampleIndex;

        MetricBucket copy() {
            MetricBucket result = new MetricBucket();
            result.requests = requests;
            result.successes = successes;
            result.failures = failures;
            result.totalLatencyMicros = totalLatencyMicros;
            result.maxLatencyMicros = maxLatencyMicros;
            System.arraycopy(samples, 0, result.samples, 0, samples.length);
            result.sampleCount = sampleCount;
            result.sampleIndex = sampleIndex;
            return result;
        }

        void record(long latencyMicros, boolean success) {
            requests++;
            if (success) {
                successes++;
            } else {
                failures++;
            }
            totalLatencyMicros += latencyMicros;
            maxLatencyMicros = Math.max(maxLatencyMicros, latencyMicros);
            samples[sampleIndex] = latencyMicros;
            sampleIndex = (sampleIndex + 1) % SAMPLE_BUFFER_SIZE;
            sampleCount = Math.min(sampleCount + 1, SAMPLE_BUFFER_SIZE);
        }

        MetricSnapshot snapshot() {
            if (requests == 0) {
                return null;
            }
            long[] sorted = Arrays.copyOf(samples, sampleCount);
            Arrays.sort(sorted);
            long p99 = sorted.length == 0 ? 0 : sorted[Math.min(sorted.length - 1, (int) (sorted.length * 0.99))];
            return new MetricSnapshot(
                    requests, successes, failures,
                    (double) totalLatencyMicros / requests / 1000.0,
                    p99 / 1000.0,
                    maxLatencyMicros / 1000.0);
        }
    }

    private static final class CollectionScope {
        private final boolean all;
        private final Set<String> enabled;

        private CollectionScope(boolean all, Set<String> enabled) {
            this.all = all;
            this.enabled = enabled;
        }

        private boolean includes(String collection) {
            return all || enabled.contains(collection);
        }
    }
}
