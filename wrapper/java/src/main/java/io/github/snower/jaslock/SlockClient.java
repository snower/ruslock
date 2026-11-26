package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackExecutorManager;
import io.github.snower.jaslock.callback.ExecutorOption;
import io.github.snower.jaslock.commands.Command;
import io.github.snower.jaslock.commands.CommandResult;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.InitCommandResult;
import io.github.snower.jaslock.commands.PingCommand;
import io.github.snower.jaslock.commands.PingCommandResult;
import io.github.snower.jaslock.exceptions.ClientAsyncCallbackDisabledException;
import io.github.snower.jaslock.exceptions.ClientClosedException;
import io.github.snower.jaslock.exceptions.SlockException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Jaslock-compatible single-client facade backed by one native client handle.
 *
 * <p>All connection ownership, handshake, request correlation, routing and
 * timeouts live in Rust. This class only keeps the compatibility surface, lazily
 * creates the native client, forwards low-level command frames and dispatches
 * callback results through the configured {@link CallbackExecutorManager}.
 *
 * <p>Construction never loads the native library: the first native operation
 * creates the runtime and client.
 */
public class SlockClient implements Runnable, ISlockClient {
    protected static final class BytesKey {
        private final byte[] bytes;
        private int bytesHash = 0;

        public BytesKey(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return Arrays.equals(bytes, ((BytesKey) o).bytes);
        }

        @Override
        public int hashCode() {
            if (bytesHash == 0) {
                bytesHash = Arrays.hashCode(bytes);
            }
            return bytesHash;
        }
    }

    protected static final class BufferedInputStream extends java.io.BufferedInputStream {

        public BufferedInputStream(InputStream in) {
            super(in, 4096);
        }

        public BufferedInputStream(InputStream in, int size) {
            super(in, size);
        }
    }

    protected static final class BufferedOutputStream extends java.io.BufferedOutputStream {

        public BufferedOutputStream(OutputStream out) {
            super(out, 4096);
        }

        public BufferedOutputStream(OutputStream out, int size) {
            super(out, size);
        }

        public void writeFlush(byte[] b) throws IOException {
            writeFlush(b, 0, b.length);
        }

        public void writeFlush(byte[] b, int off, int len) throws IOException {
            if (count == 0) {
                synchronized (this) {
                    out.write(b, off, len);
                }
            } else {
                write(b, off, len);
                synchronized (this) {
                    if (count > 0) {
                        out.write(buf, 0, count);
                        count = 0;
                    }
                }
            }
        }

        @Override
        public synchronized void flush() throws IOException {
            if (count > 0) {
                out.write(buf, 0, count);
                count = 0;
            }
        }
    }

    private final String host;
    private final int port;
    private final NativeClient nativeClient;
    private final SlockReplsetClient replsetClient;
    private final Object openLock = new Object();
    private final Object closeLock = new Object();
    private final Object callbackLock = new Object();

    private volatile short defaultTimeoutFlag;
    private volatile short defaultExpriedFlag;
    private volatile boolean closed;
    private volatile boolean opened;
    private SlockDatabase[] databases;
    private volatile CallbackExecutorManager callbackExecutorManager;

    public SlockClient() {
        this("127.0.0.1", 5658);
    }

    public SlockClient(String host) {
        this(host, 5658);
    }

    public SlockClient(String host, int port) {
        this(NativeClient.bridgeBackend(), host, port);
    }

    SlockClient(NativeClient.Backend backend, String host, int port) {
        this(backend, host, port, null, null);
    }

    private SlockClient(NativeClient.Backend backend, String host, int port,
                        SlockReplsetClient replsetClient, SlockDatabase[] databases) {
        this.host = host;
        this.port = port;
        this.replsetClient = replsetClient;
        this.nativeClient = new NativeClient(backend, host, port);
        this.databases = databases != null ? databases : new SlockDatabase[256];
    }

    public SlockClient(String host, int port, boolean enableAsyncCallback) {
        this(host, port);
        if (enableAsyncCallback) {
            this.enableAsyncCallback();
        }
    }

    /**
     * Legacy replica-set child constructor. The native client owns replica-set
     * routing now, so the shared database array is retained for compatibility but
     * no child transport or request map is created.
     */
    protected SlockClient(String host, int port, SlockReplsetClient replsetClient, SlockDatabase[] databases) {
        this(NativeClient.bridgeBackend(), host, port, replsetClient, databases);
    }

    @Override
    public boolean enableAsyncCallback() {
        if (replsetClient != null) {
            return false;
        }
        synchronized (callbackLock) {
            if (callbackExecutorManager != null) {
                return false;
            }
            CallbackExecutorManager manager =
                    new CallbackExecutorManager(ExecutorOption.DefaultOption);
            callbackExecutorManager = manager;
            if (opened) {
                manager.start();
            }
            return true;
        }
    }

    @Override
    public boolean enableAsyncCallback(ExecutorOption executorOption) {
        if (replsetClient != null) {
            return false;
        }
        synchronized (callbackLock) {
            if (callbackExecutorManager != null) {
                return false;
            }
            CallbackExecutorManager manager = new CallbackExecutorManager(executorOption);
            callbackExecutorManager = manager;
            if (opened) {
                manager.start();
            }
            return true;
        }
    }

    @Override
    public boolean enableAsyncCallback(CallbackExecutorManager callbackExecutorManager) {
        CallbackExecutorManager previous;
        synchronized (callbackLock) {
            previous = this.callbackExecutorManager;
            this.callbackExecutorManager = callbackExecutorManager;
        }
        if (previous != null) {
            stopManager(previous);
        }
        return true;
    }

    @Override
    public void setDefaultTimeoutFlag(short defaultTimeoutFlag) {
        this.defaultTimeoutFlag = defaultTimeoutFlag;
        for (SlockDatabase database : databaseSnapshot()) {
            if (database != null) {
                database.setDefaultTimeoutFlag(defaultTimeoutFlag);
            }
        }
    }

    @Override
    public void setDefaultExpriedFlag(short defaultExpriedFlag) {
        this.defaultExpriedFlag = defaultExpriedFlag;
        for (SlockDatabase database : databaseSnapshot()) {
            if (database != null) {
                database.setDefaultExpriedFlag(defaultExpriedFlag);
            }
        }
    }

    @Override
    public void open() throws IOException {
        synchronized (openLock) {
            if (opened) {
                return;
            }
            connect();
        }
        startAsyncCallbacks();
    }

    @Override
    public ISlockClient tryOpen() {
        boolean connected;
        synchronized (openLock) {
            if (opened) {
                connected = true;
            } else {
                try {
                    connected = nativeClient.tryOpen();
                } catch (IOException e) {
                    // The native client owns background reconnection; report the
                    // initial attempt as not connected just like the reference.
                    connected = false;
                }
                opened = true;
            }
        }
        startAsyncCallbacks();
        return connected ? this : null;
    }

    @Override
    public void close() {
        synchronized (closeLock) {
            if (closed) {
                return;
            }
            closed = true;
        }

        if (replsetClient == null) {
            SlockDatabase[] snapshot;
            synchronized (this) {
                snapshot = databases.clone();
                Arrays.fill(databases, null);
            }
            for (SlockDatabase database : snapshot) {
                if (database != null) {
                    database.close();
                }
            }
        }

        nativeClient.close();

        CallbackExecutorManager manager;
        synchronized (callbackLock) {
            manager = callbackExecutorManager;
            callbackExecutorManager = null;
        }
        if (replsetClient == null) {
            stopManager(manager);
        }
    }

    @Override
    public CommandResult sendCommand(Command command) throws SlockException {
        if (closed) {
            throw new ClientClosedException("client has been closed");
        }
        return NativeClient.sendAndWait(nativeClient, command);
    }

    @Override
    public void sendCommand(Command command, Consumer<CallbackCommandResult> callback)
            throws SlockException {
        if (closed) {
            throw new ClientClosedException("client has been closed");
        }
        CallbackExecutorManager manager = callbackExecutorManager;
        if (manager == null) {
            throw new ClientAsyncCallbackDisabledException(
                    "The asynchronous thread pool is not enabled. First enableAsyncCallback"
                            + " to enable the asynchronous thread pool.");
        }
        NativeClient.sendAsync(nativeClient, manager, command, callback);
    }

    @Override
    public void writeCommand(Command command) throws SlockException {
        if (closed) {
            throw new ClientClosedException("client has been closed");
        }
        NativeClient.writeOnly(nativeClient, command);
    }

    @Override
    public boolean ping() throws SlockException {
        PingCommand pingCommand = new PingCommand();
        PingCommandResult pingCommandResult = (PingCommandResult) sendCommand(pingCommand);
        return pingCommandResult != null
                && pingCommandResult.getResult() == ICommand.COMMAND_RESULT_SUCCED;
    }

    @Override
    public SlockDatabase selectDatabase(byte dbId) {
        int index = dbId & 0xff;
        if (databases[index] == null) {
            synchronized (this) {
                if (databases[index] == null) {
                    databases[index] = new SlockDatabase(
                            this, dbId, defaultTimeoutFlag, defaultExpriedFlag);
                }
            }
        }
        return databases[index];
    }

    /**
     * Legacy reader-loop entry. Native owns the read loop; the method remains for
     * source and binary compatibility.
     */
    @Override
    public void run() {
    }

    // ------------------------------------------------------------------
    // Preserved protected transport hooks
    // ------------------------------------------------------------------

    /**
     * Strict connect. The native client performs the handshake and keeps
     * background reconnection state.
     */
    protected void connect() throws IOException {
        nativeClient.open();
        opened = true;
    }

    /** Native close owns the socket; explicit close is handled by {@link #close()}. */
    protected void closeSocket() {
    }

    /**
     * Legacy handshake hook. Native open already performs the handshake, so there
     * is no separate Java init exchange.
     */
    protected InitCommandResult initClient() throws IOException {
        return null;
    }

    /** Native correlates responses; preserved for subclass compatibility. */
    protected void handleCommand(CommandResult commandResult) {
    }

    /** Native tracks leader changes; preserved for subclass compatibility. */
    protected void handleInitCommand(InitCommandResult initCommandResult) {
    }

    /** Native owns reconnection; preserved for subclass compatibility. */
    protected void reconnect() {
    }

    // ------------------------------------------------------------------
    // Package-private facade seams
    // ------------------------------------------------------------------

    /** Package-private seam used by {@link SlockDatabase}. */
    NativeClient nativeClient() {
        return nativeClient;
    }

    /** Package-private seam used by {@link SlockDatabase}. */
    CallbackExecutorManager asyncCallbackManager() {
        return callbackExecutorManager;
    }

    // ------------------------------------------------------------------
    // Factory delegates
    // ------------------------------------------------------------------

    @Override
    public Lock newLock(byte[] lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newLock(lockKey, timeout, expried);
    }

    @Override
    public Lock newLock(String lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newLock(lockKey, timeout, expried);
    }

    @Override
    public Event newEvent(byte[] eventKey, int timeout, int expried, boolean defaultSeted) {
        return selectDatabase((byte) 0).newEvent(eventKey, timeout, expried, defaultSeted);
    }

    @Override
    public Event newEvent(String eventKey, int timeout, int expried, boolean defaultSeted) {
        return selectDatabase((byte) 0).newEvent(eventKey, timeout, expried, defaultSeted);
    }

    @Override
    public ReentrantLock newReentrantLock(byte[] lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newReentrantLock(lockKey, timeout, expried);
    }

    @Override
    public ReentrantLock newReentrantLock(String lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newReentrantLock(lockKey, timeout, expried);
    }

    @Override
    public ReadWriteLock newReadWriteLock(byte[] lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newReadWriteLock(lockKey, timeout, expried);
    }

    @Override
    public ReadWriteLock newReadWriteLock(String lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newReadWriteLock(lockKey, timeout, expried);
    }

    @Override
    public Semaphore newSemaphore(byte[] semaphoreKey, short count, int timeout, int expried) {
        return selectDatabase((byte) 0).newSemaphore(semaphoreKey, count, timeout, expried);
    }

    @Override
    public Semaphore newSemaphore(String semaphoreKey, short count, int timeout, int expried) {
        return selectDatabase((byte) 0).newSemaphore(semaphoreKey, count, timeout, expried);
    }

    @Override
    public MaxConcurrentFlow newMaxConcurrentFlow(byte[] flowKey, short count, int timeout, int expried) {
        return selectDatabase((byte) 0).newMaxConcurrentFlow(flowKey, count, timeout, expried);
    }

    @Override
    public MaxConcurrentFlow newMaxConcurrentFlow(String flowKey, short count, int timeout, int expried) {
        return selectDatabase((byte) 0).newMaxConcurrentFlow(flowKey, count, timeout, expried);
    }

    @Override
    public MaxConcurrentFlow newMaxConcurrentFlow(byte[] flowKey, short count, int timeout, int expried, byte priority) {
        return selectDatabase((byte) 0).newMaxConcurrentFlow(flowKey, count, timeout, expried, priority);
    }

    @Override
    public MaxConcurrentFlow newMaxConcurrentFlow(String flowKey, short count, int timeout, int expried, byte priority) {
        return selectDatabase((byte) 0).newMaxConcurrentFlow(flowKey, count, timeout, expried, priority);
    }

    @Override
    public TokenBucketFlow newTokenBucketFlow(byte[] flowKey, short count, int timeout, double period) {
        return selectDatabase((byte) 0).newTokenBucketFlow(flowKey, count, timeout, period);
    }

    @Override
    public TokenBucketFlow newTokenBucketFlow(String flowKey, short count, int timeout, double period) {
        return selectDatabase((byte) 0).newTokenBucketFlow(flowKey, count, timeout, period);
    }

    @Override
    public TokenBucketFlow newTokenBucketFlow(byte[] flowKey, short count, int timeout, double period, byte priority) {
        return selectDatabase((byte) 0).newTokenBucketFlow(flowKey, count, timeout, period, priority);
    }

    @Override
    public TokenBucketFlow newTokenBucketFlow(String flowKey, short count, int timeout, double period, byte priority) {
        return selectDatabase((byte) 0).newTokenBucketFlow(flowKey, count, timeout, period, priority);
    }

    @Override
    public GroupEvent newGroupEvent(byte[] groupKey, long clientId, long versionId, int timeout, int expried) {
        return selectDatabase((byte) 0).newGroupEvent(groupKey, clientId, versionId, timeout, expried);
    }

    @Override
    public GroupEvent newGroupEvent(String groupKey, long clientId, long versionId, int timeout, int expried) {
        return selectDatabase((byte) 0).newGroupEvent(groupKey, clientId, versionId, timeout, expried);
    }

    @Override
    public TreeLock newTreeLock(byte[] parentKey, byte[] lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newTreeLock(parentKey, lockKey, timeout, expried);
    }

    @Override
    public TreeLock newTreeLock(String parentKey, String lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newTreeLock(parentKey, lockKey, timeout, expried);
    }

    @Override
    public TreeLock newTreeLock(byte[] lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newTreeLock(lockKey, timeout, expried);
    }

    @Override
    public TreeLock newTreeLock(String lockKey, int timeout, int expried) {
        return selectDatabase((byte) 0).newTreeLock(lockKey, timeout, expried);
    }

    @Override
    public PriorityLock newPriorityLock(byte[] lockKey, byte priority, int timeout, int expried) {
        return selectDatabase((byte) 0).newPriorityLock(lockKey, priority, timeout, expried);
    }

    @Override
    public PriorityLock newPriorityLock(String lockKey, byte priority, int timeout, int expried) {
        return selectDatabase((byte) 0).newPriorityLock(lockKey, priority, timeout, expried);
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private SlockDatabase[] databaseSnapshot() {
        synchronized (this) {
            return databases.clone();
        }
    }

    private void startAsyncCallbacks() {
        CallbackExecutorManager manager = callbackExecutorManager;
        if (manager != null) {
            manager.start();
        }
    }

    /**
     * Stops a manager without blocking when the caller is one of its callbacks.
     * The reference manager awaits termination for up to 60 seconds, which would
     * stall a reentrant close until that timeout.
     */
    private static void stopManager(CallbackExecutorManager manager) {
        if (manager == null) {
            return;
        }
        if (NativeClient.isInCallback()) {
            Thread thread = new Thread(manager::stop, "ruslock-callback-stop");
            thread.setDaemon(true);
            thread.start();
        } else {
            manager.stop();
        }
    }
}
