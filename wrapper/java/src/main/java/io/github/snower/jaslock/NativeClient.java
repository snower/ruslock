package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackExecutorManager;
import io.github.snower.jaslock.commands.Command;
import io.github.snower.jaslock.commands.CommandResult;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.InitCommandResult;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.commands.LockCommandResult;
import io.github.snower.jaslock.commands.PingCommandResult;
import io.github.snower.jaslock.exceptions.ClientAsyncCallbackDisabledException;
import io.github.snower.jaslock.exceptions.ClientAsyncCallbackStopedException;
import io.github.snower.jaslock.exceptions.ClientClosedException;
import io.github.snower.jaslock.exceptions.ClientCommandException;
import io.github.snower.jaslock.exceptions.ClientCommandTimeoutException;
import io.github.snower.jaslock.exceptions.ClientOutputStreamException;
import io.github.snower.jaslock.exceptions.ClientUnconnectException;
import io.github.snower.jaslock.exceptions.SlockException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Package-private native client seam shared by {@link SlockClient} and
 * {@link SlockReplsetClient}.
 *
 * <p>Both concrete facade classes own exactly one native client handle. A
 * single-node facade creates it with one {@code host:port} entry; the replica-set
 * facade passes the complete node list to Rust, which owns routing, fail-over and
 * pending-request tracking. No Java child {@code SlockClient} transport and no
 * Java request map are used.
 *
 * <p>The class is deliberately lazy: construction only records the node list.
 * {@link NativeBridge} (and therefore {@code NativeLibrary.load()}) is touched on
 * the first native operation, never from a client or database constructor.
 */
final class NativeClient {

    /**
     * Thin seam consumed by the JNI bridge. Tests replace it with a fake so the
     * facade can be exercised without loading the native library.
     */
    interface Backend {
        long runtimeCreate();

        long clientCreateNodes(long runtime, String[] nodes);

        void clientOpen(long client) throws IOException;

        int clientTryOpen(long client) throws IOException;

        void clientClose(long client);

        boolean clientIsClosed(long client);

        long databaseCreate(long client, int dbId);

        void databaseSetDefaultFlags(long database, int timeoutFlags, int expiredFlags);

        byte[] sendCommand(long client, byte[] frame, long timeoutMillis) throws IOException;

        void writeCommand(long client, byte[] frame) throws IOException;

        long sendRawSubmit(long client, byte[] frame, long timeoutMillis) throws IOException;

        byte[] sendRawAwait(long completion) throws IOException;

        boolean completionIsDone(long completion);

        long completionEpoch();

        long awaitCompletionChange(long observed, long timeoutMillis);

        void signalCompletionPump();

        boolean release(long handle);
    }

    /** Default adapter to the package-private JNI entry points. */
    static final class BridgeBackend implements Backend {
        @Override
        public long runtimeCreate() {
            return NativeBridge.runtimeCreate();
        }

        @Override
        public long clientCreateNodes(long runtime, String[] nodes) {
            return NativeBridge.clientCreateNodes(runtime, nodes);
        }

        @Override
        public void clientOpen(long client) throws IOException {
            NativeBridge.clientOpen(client);
        }

        @Override
        public int clientTryOpen(long client) throws IOException {
            return NativeBridge.clientTryOpen(client);
        }

        @Override
        public void clientClose(long client) {
            NativeBridge.clientClose(client);
        }

        @Override
        public boolean clientIsClosed(long client) {
            return NativeBridge.clientIsClosed(client);
        }

        @Override
        public long databaseCreate(long client, int dbId) {
            return NativeBridge.databaseCreate(client, dbId);
        }

        @Override
        public void databaseSetDefaultFlags(long database, int timeoutFlags, int expiredFlags) {
            NativeBridge.databaseSetDefaultFlags(database, timeoutFlags, expiredFlags);
        }

        @Override
        public byte[] sendCommand(long client, byte[] frame, long timeoutMillis)
                throws IOException {
            return NativeBridge.sendCommand(client, frame, timeoutMillis);
        }

        @Override
        public void writeCommand(long client, byte[] frame) throws IOException {
            NativeBridge.writeCommand(client, frame);
        }

        @Override
        public long sendRawSubmit(long client, byte[] frame, long timeoutMillis)
                throws IOException {
            return NativeBridge.sendRawSubmit(client, frame, timeoutMillis);
        }

        @Override
        public byte[] sendRawAwait(long completion) throws IOException {
            return NativeBridge.sendRawAwait(completion);
        }

        @Override
        public boolean completionIsDone(long completion) {
            return NativeBridge.completionIsDone(completion);
        }

        @Override
        public long completionEpoch() {
            return NativeBridge.completionEpoch();
        }

        @Override
        public long awaitCompletionChange(long observed, long timeoutMillis) {
            return NativeBridge.awaitCompletionChange(observed, timeoutMillis);
        }

        @Override
        public void signalCompletionPump() {
            NativeBridge.signalCompletionPump();
        }

        @Override
        public boolean release(long handle) {
            return NativeBridge.release(handle);
        }
    }

    private static final String ASYNC_DISABLED_MESSAGE =
            "The asynchronous thread pool is not enabled. First enableAsyncCallback"
                    + " to enable the asynchronous thread pool.";
    private static final String ASYNC_STOPPED_MESSAGE = "Async thread pool stopped";

    private static final Backend BRIDGE_BACKEND = new BridgeBackend();

    /**
     * Raw asynchronous completions that have been submitted but whose native
     * result is not ready yet. One shared daemon pump watches the process-wide
     * native completion epoch, scans only already-ready results and hands them
     * to the configured callback pool. No thread is parked per request: pending
     * installs/acquires that wait for a server event consume no Java thread at
     * all until their result becomes ready.
     */
    private static final ConcurrentLinkedQueue<PendingRawCompletion> PENDING_RAW =
            new ConcurrentLinkedQueue<>();
    private static final AtomicBoolean RAW_PUMP_STARTED = new AtomicBoolean(false);

    /**
     * Guards the pump's "queue empty" wait only. Registration notifies it after
     * queuing, so an idle pump parks without any native call.
     */
    private static final Object RAW_PUMP_MONITOR = new Object();

    /**
     * Negative means "wait until the native epoch changes". This is a single
     * {@code Condvar} wait, not a periodic sleep; every native completion bumps
     * the epoch after its result becomes visible and every registration signals
     * the pump, so no ready token can be stranded.
     */
    private static final long RAW_PUMP_WAIT_MILLIS = -1L;
    private static final long RAW_PUMP_FALLBACK_WAIT_MILLIS = 1000L;

    /**
     * The reference callback manager has no public submit entry and the frozen
     * public/protected signature baseline forbids adding one. Read the two
     * existing protected dispatcher fields instead; custom managers passed to
     * {@code enableAsyncCallback(CallbackExecutorManager)} are therefore honoured
     * without changing their API.
     */
    private static final Field CALLBACK_EXECUTOR_FIELD = managerField("callbackExecutor");
    private static final Field CALLBACK_RUNNING_FIELD = managerField("isRuning");

    /** Marks a thread currently running a facade callback, for reentrant close. */
    private static final ThreadLocal<Boolean> IN_CALLBACK = new ThreadLocal<>();

    private final Backend backend;
    private final String[] nodes;
    private final Object lock = new Object();

    private long runtime;
    private long client;
    private boolean closed;

    NativeClient(Backend backend, String host, int port) {
        this(backend, new String[]{address(host, port)});
    }

    NativeClient(Backend backend, String[] nodes) {
        this.backend = backend;
        this.nodes = nodes == null ? new String[0] : nodes.clone();
    }

    static Backend bridgeBackend() {
        return BRIDGE_BACKEND;
    }

    /** Returns the client handle, creating the native client on first use. */
    long clientHandle() {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("client has been closed");
            }
            if (client != 0) {
                return client;
            }
            if (nodes.length == 0) {
                throw new IllegalStateException("client node list is empty");
            }
            if (runtime == 0) {
                runtime = requireHandle(backend.runtimeCreate(), "runtimeCreate");
            }
            client = requireHandle(backend.clientCreateNodes(runtime, nodes), "clientCreateNodes");
            return client;
        }
    }

    void open() throws IOException {
        if (nodes.length == 0) {
            throw new IOException("client node list is empty");
        }
        backend.clientOpen(clientHandle());
    }

    boolean tryOpen() throws IOException {
        if (nodes.length == 0) {
            throw new IOException("client node list is empty");
        }
        return backend.clientTryOpen(clientHandle()) != 0;
    }

    boolean isClosed() {
        synchronized (lock) {
            if (closed) {
                return true;
            }
            long handle = client;
            return handle != 0 && backend.clientIsClosed(handle);
        }
    }

    void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            long clientHandle = client;
            client = 0;
            if (clientHandle != 0) {
                try {
                    backend.clientClose(clientHandle);
                } catch (RuntimeException ignored) {
                    // Close is idempotent; release below still invalidates the handle.
                }
                releaseQuietly(clientHandle);
            }
            long runtimeHandle = runtime;
            runtime = 0;
            if (runtimeHandle != 0) {
                releaseQuietly(runtimeHandle);
            }
        }
    }

    long createDatabase(byte dbId, short timeoutFlag, short expriedFlag) {
        long database = requireHandle(
                backend.databaseCreate(clientHandle(), dbId & 0xff), "databaseCreate");
        try {
            backend.databaseSetDefaultFlags(
                    database, timeoutFlag & 0xffff, expriedFlag & 0xffff);
            return database;
        } catch (RuntimeException e) {
            releaseQuietly(database);
            throw e;
        }
    }

    void databaseDefaultFlagsChanged(long database, short timeoutFlag, short expriedFlag) {
        if (database == 0) {
            return;
        }
        backend.databaseSetDefaultFlags(database, timeoutFlag & 0xffff, expriedFlag & 0xffff);
    }

    void release(long handle) {
        if (handle == 0) {
            return;
        }
        backend.release(handle);
    }

    private void releaseQuietly(long handle) {
        try {
            release(handle);
        } catch (RuntimeException ignored) {
            // Best effort: a failed release must not mask close().
        }
    }

    private static long requireHandle(long handle, String operation) {
        if (handle == 0) {
            throw new IllegalStateException("native " + operation + " returned no handle");
        }
        return handle;
    }

    private static String address(String host, int port) {
        String value = host == null || host.isEmpty() ? "127.0.0.1" : host;
        if (value.indexOf(':') >= 0 && !(value.startsWith("[") && value.endsWith("]"))) {
            return "[" + value + "]:" + port;
        }
        return value + ":" + port;
    }

    // ------------------------------------------------------------------
    // Low-level command transport
    // ------------------------------------------------------------------

    /** Synchronous {@code sendCommand}: submits, waits and parses the response. */
    static CommandResult sendAndWait(NativeClient nativeClient, Command command)
            throws SlockException {
        byte[] response = nativeClient.send(frame(command), deadlineMillis(command));
        CommandResult result = parseResponse(response);
        command.commandResult = result;
        return result;
    }

    /** Synchronous {@code writeCommand}: writes and flushes without a response. */
    static void writeOnly(NativeClient nativeClient, Command command) throws SlockException {
        nativeClient.write(frame(command));
    }

    /**
     * Asynchronous {@code sendCommand(Command, Consumer)}.
     *
     * <p>An immediate transport rejection is thrown to the caller and never
     * reaches the callback. Once the native submit has been accepted, the
     * completion is queued and delivered to the callback exactly once on the
     * configured {@link CallbackExecutorManager}. A single shared readiness pump
     * waits on the native completion epoch and only awaits results that are
     * already done, so N pending commands do not allocate N waiting threads. The
     * native total deadline already contains the legacy 120-second command
     * window, so no Java timer adds a second one.
     */
    static void sendAsync(NativeClient nativeClient, CallbackExecutorManager manager,
                          Command command, Consumer<CallbackCommandResult> callback)
            throws SlockException {
        requireExecutor(manager);
        byte[] frame = frame(command);
        long deadline = deadlineMillis(command);
        long completion = nativeClient.submit(frame, deadline);

        PendingRawCompletion pending = new PendingRawCompletion(
                nativeClient, manager, command, callback, completion);

        boolean immediate;
        try {
            immediate = nativeClient.isCompletionDone(completion);
        } catch (RuntimeException e) {
            immediate = false;
        }
        if (immediate) {
            // Already complete: deliver without a queue round trip. A dispatch
            // failure still delivers inline so the accepted result is not lost.
            pending.dispatchWhenReady();
            return;
        }

        PENDING_RAW.add(pending);
        try {
            ensureRawPump();
        } catch (RuntimeException e) {
            // The pump could not start; deliver this accepted completion through
            // the callback dispatcher (or inline) instead of leaving it queued.
            if (PENDING_RAW.remove(pending)) {
                pending.dispatchWhenReady();
            }
            return;
        }
        // Queue-then-signal ordering: a pump that already scanned the queue is
        // woken by the changed epoch, and a registration that lands after the
        // signal is still visible to the next scan. The monitor wake only covers
        // the pump's empty-queue wait and never carries state.
        try {
            nativeClient.signalCompletionPump();
        } catch (RuntimeException ignored) {
            // The token's own completion bumps the epoch, so a lost explicit
            // signal only delays the scan and never strands the completion.
        }
        synchronized (RAW_PUMP_MONITOR) {
            RAW_PUMP_MONITOR.notifyAll();
        }
    }

    private static void ensureRawPump() {
        if (!RAW_PUMP_STARTED.compareAndSet(false, true)) {
            return;
        }
        Thread pump = new Thread(NativeClient::runRawPump, "ruslock-raw-completion");
        pump.setDaemon(true);
        try {
            pump.start();
        } catch (RuntimeException e) {
            RAW_PUMP_STARTED.set(false);
            throw e;
        }
    }

    private static void runRawPump() {
        while (true) {
            if (PENDING_RAW.isEmpty()) {
                awaitRegistration();
                continue;
            }

            PendingRawCompletion head = PENDING_RAW.peek();
            if (head == null) {
                continue;
            }

            // Read the epoch before scanning so a completion that lands during
            // the scan either shows up as ready or changes the epoch and
            // releases the wait below immediately.
            long observed;
            try {
                observed = head.nativeClient.completionEpoch();
            } catch (RuntimeException e) {
                observed = 0L;
            }

            boolean progressed;
            try {
                progressed = dispatchReadyRaw();
            } catch (RuntimeException e) {
                progressed = false;
            }
            if (progressed) {
                // Drain a burst without an extra wait.
                continue;
            }
            if (PENDING_RAW.isEmpty()) {
                continue;
            }

            PendingRawCompletion waitOn = PENDING_RAW.peek();
            if (waitOn == null) {
                continue;
            }
            try {
                waitOn.nativeClient.awaitCompletionChange(
                        observed, RAW_PUMP_WAIT_MILLIS);
            } catch (RuntimeException e) {
                // Keep the pump alive. A bounded wait avoids a hot spin if the
                // native signal is temporarily unavailable.
                try {
                    waitOn.nativeClient.awaitCompletionChange(
                            observed, RAW_PUMP_FALLBACK_WAIT_MILLIS);
                } catch (RuntimeException ignored) {
                    Thread.yield();
                }
            }
        }
    }

    private static void awaitRegistration() {
        synchronized (RAW_PUMP_MONITOR) {
            while (PENDING_RAW.isEmpty()) {
                try {
                    RAW_PUMP_MONITOR.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static boolean dispatchReadyRaw() {
        boolean progressed = false;
        for (PendingRawCompletion pending : PENDING_RAW) {
            boolean ready;
            try {
                ready = pending.nativeClient.isCompletionDone(pending.completion);
            } catch (RuntimeException e) {
                if (PENDING_RAW.remove(pending)) {
                    // Surface the failure through the normal completion path so
                    // the callback still sees an error instead of being dropped.
                    pending.dispatchWhenReady();
                }
                progressed = true;
                continue;
            }
            if (!ready) {
                continue;
            }
            if (PENDING_RAW.remove(pending)) {
                pending.dispatchWhenReady();
                progressed = true;
            }
        }
        return progressed;
    }

    private static final class PendingRawCompletion {
        private final NativeClient nativeClient;
        private final CallbackExecutorManager manager;
        private final Command command;
        private final Consumer<CallbackCommandResult> callback;
        private final long completion;
        private final AtomicBoolean finished = new AtomicBoolean(false);

        private PendingRawCompletion(NativeClient nativeClient,
                                     CallbackExecutorManager manager,
                                     Command command,
                                     Consumer<CallbackCommandResult> callback,
                                     long completion) {
            this.nativeClient = nativeClient;
            this.manager = manager;
            this.command = command;
            this.callback = callback;
            this.completion = completion;
        }

        private void dispatchWhenReady() {
            if (!finished.compareAndSet(false, true)) {
                return;
            }
            completeAsync(nativeClient, manager, command, callback, completion);
        }
    }

    private byte[] send(byte[] frame, long timeoutMillis) throws SlockException {
        try {
            return backend.sendCommand(clientHandle(), frame, timeoutMillis);
        } catch (IOException e) {
            throw mapIo(e, false);
        } catch (RuntimeException e) {
            throw mapRuntime(e);
        }
    }

    private void write(byte[] frame) throws SlockException {
        try {
            backend.writeCommand(clientHandle(), frame);
        } catch (IOException e) {
            throw mapIo(e, true);
        } catch (RuntimeException e) {
            throw mapRuntime(e);
        }
    }

    private long submit(byte[] frame, long timeoutMillis) throws SlockException {
        try {
            return requireHandle(
                    backend.sendRawSubmit(clientHandle(), frame, timeoutMillis),
                    "sendRawSubmit");
        } catch (IOException e) {
            throw mapIo(e, false);
        } catch (RuntimeException e) {
            throw mapRuntime(e);
        }
    }

    private byte[] await(long completion) throws SlockException {
        try {
            return backend.sendRawAwait(completion);
        } catch (IOException e) {
            throw mapIo(e, false);
        } catch (RuntimeException e) {
            throw mapRuntime(e);
        }
    }

    /** Non-blocking readiness probe used by the shared raw completion pump. */
    boolean isCompletionDone(long completion) {
        return backend.completionIsDone(completion);
    }

    /** Process-wide native completion generation, non-blocking. */
    long completionEpoch() {
        return backend.completionEpoch();
    }

    /** Waits for the native epoch to change; pump thread only. */
    long awaitCompletionChange(long observed, long timeoutMillis) {
        return backend.awaitCompletionChange(observed, timeoutMillis);
    }

    /** Wake-up signal for the shared pump; carries no state. */
    void signalCompletionPump() {
        backend.signalCompletionPump();
    }

    private static void completeAsync(NativeClient nativeClient, CallbackExecutorManager manager,
                                      Command command, Consumer<CallbackCommandResult> callback,
                                      long completion) {
        CommandResult result = null;
        SlockException error = null;
        try {
            result = parseResponse(nativeClient.await(completion));
        } catch (SlockException e) {
            error = e;
        } catch (RuntimeException e) {
            error = mapRuntime(e);
        } finally {
            nativeClient.releaseQuietly(completion);
        }
        command.commandResult = result;
        command.exception = error;
        invokeCallback(manager, callback, new CallbackCommandResult(command, result, error));
    }

    /**
     * Returns the legacy client wait for a command. The value is the total
     * deadline and already contains the historical +120 second grace; the native
     * transport must not add a second grace period.
     */
    static long deadlineMillis(Command command) {
        long seconds = 120L;
        if (command instanceof LockCommand) {
            seconds = (((LockCommand) command).getTimeout() & 0xffffL) + 120L;
        }
        return seconds * 1000L;
    }

    static byte[] frame(Command command) throws SlockException {
        byte[] header = command.dumpCommand();
        if (header == null || header.length != 64) {
            throw new ClientCommandException(
                    "command frame must contain a 64-byte header");
        }
        byte[] extra = command.getExtraData();
        if (extra == null || extra.length == 0) {
            return header;
        }
        byte[] frame = new byte[header.length + extra.length];
        System.arraycopy(header, 0, frame, 0, header.length);
        System.arraycopy(extra, 0, frame, header.length, extra.length);
        return frame;
    }

    static CommandResult parseResponse(byte[] frame) throws SlockException {
        if (frame == null) {
            throw new ClientCommandException("native response frame is null");
        }
        if (frame.length < 64) {
            throw new ClientCommandException("native response frame is shorter than 64 bytes");
        }
        byte[] header = Arrays.copyOf(frame, 64);
        byte[] extra = frame.length > 64 ? Arrays.copyOfRange(frame, 64, frame.length) : null;
        switch (header[2]) {
            case ICommand.COMMAND_TYPE_LOCK:
            case ICommand.COMMAND_TYPE_UNLOCK: {
                LockCommandResult result = new LockCommandResult();
                result.parseCommand(header);
                if (extra != null) {
                    result.loadCommandData(extra);
                } else if (result.hasExtraData()) {
                    throw new ClientCommandException(
                            "lock response is missing its extra data frame");
                }
                return result;
            }
            case ICommand.COMMAND_TYPE_PING: {
                PingCommandResult result = new PingCommandResult();
                result.parseCommand(header);
                return result;
            }
            case ICommand.COMMAND_TYPE_INIT: {
                InitCommandResult result = new InitCommandResult();
                result.parseCommand(header);
                return result;
            }
            default: {
                CommandResult result = new CommandResult();
                result.parseCommand(header);
                return result;
            }
        }
    }

    // ------------------------------------------------------------------
    // Callback dispatch
    // ------------------------------------------------------------------

    /**
     * Checks that the manager exists and is running. Used before a native submit
     * so a stopped callback configuration fails without sending a command.
     */
    static ExecutorService requireExecutor(CallbackExecutorManager manager) throws SlockException {
        if (manager == null) {
            throw new ClientAsyncCallbackDisabledException(ASYNC_DISABLED_MESSAGE);
        }
        ExecutorService executor = executorIfRunning(manager);
        if (executor == null) {
            throw new ClientAsyncCallbackStopedException(ASYNC_STOPPED_MESSAGE);
        }
        return executor;
    }

    /** Dispatches a database-level completion task on the configured executor. */
    static void dispatch(CallbackExecutorManager manager, Runnable task) throws SlockException {
        ExecutorService executor = requireExecutor(manager);
        Runnable wrapped = () -> runCallback(task);
        try {
            executor.execute(wrapped);
        } catch (RejectedExecutionException e) {
            throw new ClientAsyncCallbackStopedException(ASYNC_STOPPED_MESSAGE);
        } catch (RuntimeException e) {
            throw new SlockException("failed to dispatch asynchronous completion: " + e);
        }
    }

    /**
     * Delivers exactly one callback result. If the executor disappeared between
     * native acceptance and completion, the callback runs on the completion
     * thread instead of being lost.
     */
    private static void invokeCallback(CallbackExecutorManager manager,
                                       Consumer<CallbackCommandResult> callback,
                                       CallbackCommandResult result) {
        Runnable task = () -> runCallback(() -> callback.accept(result));
        ExecutorService executor = executorIfRunning(manager);
        if (executor == null) {
            task.run();
            return;
        }
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            task.run();
        } catch (RuntimeException e) {
            task.run();
        }
    }

    private static void runCallback(Runnable task) {
        Boolean previous = IN_CALLBACK.get();
        IN_CALLBACK.set(Boolean.TRUE);
        try {
            task.run();
        } catch (RuntimeException ignored) {
            // A user callback must not terminate the dispatcher.
        } finally {
            if (previous == null) {
                IN_CALLBACK.remove();
            } else {
                IN_CALLBACK.set(previous);
            }
        }
    }

    static boolean isInCallback() {
        return Boolean.TRUE.equals(IN_CALLBACK.get());
    }

    private static ExecutorService executorIfRunning(CallbackExecutorManager manager) {
        if (manager == null) {
            return null;
        }
        try {
            if (!CALLBACK_RUNNING_FIELD.getBoolean(manager)) {
                return null;
            }
            Object executor = CALLBACK_EXECUTOR_FIELD.get(manager);
            return executor instanceof ExecutorService ? (ExecutorService) executor : null;
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    private static Field managerField(String name) {
        try {
            Field field = CallbackExecutorManager.class.getDeclaredField(name);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ------------------------------------------------------------------
    // Error mapping
    // ------------------------------------------------------------------

    static SlockException mapIo(IOException error, boolean write) {
        String message = message(error);
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("closed")) {
            return new ClientClosedException(message);
        }
        if (write) {
            if (lower.contains("not connected") || lower.contains("client disconnected")) {
                return new ClientUnconnectException(message);
            }
            // Legacy writeCommand reports a failed flush as an output-stream error.
            return new ClientOutputStreamException(message);
        }
        if (lower.contains("timeout") || lower.contains("timed out")
                || lower.contains("wait budget") || lower.contains("exceeded")) {
            return new ClientCommandTimeoutException(message);
        }
        if (lower.contains("protocol")) {
            return new ClientCommandException(message);
        }
        if (lower.contains("connect") || lower.contains("refused")
                || lower.contains("not connected") || lower.contains("disconnected")
                || lower.contains("broken pipe") || lower.contains("reset")
                || lower.contains("io error")) {
            return new ClientUnconnectException(message);
        }
        return new ClientUnconnectException(message);
    }

    static SlockException mapRuntime(RuntimeException error) {
        String message = message(error);
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("closed")) {
            return new ClientClosedException(message);
        }
        if (lower.contains("timeout") || lower.contains("timed out")
                || lower.contains("wait budget")) {
            return new ClientCommandTimeoutException(message);
        }
        if (lower.contains("connect") || lower.contains("not connected")) {
            return new ClientUnconnectException(message);
        }
        if (error instanceof IllegalArgumentException) {
            return new ClientCommandException(message);
        }
        return new SlockException(message);
    }

    private static String message(Throwable error) {
        String message = error.getMessage();
        return message == null || message.isEmpty() ? error.toString() : message;
    }
}
