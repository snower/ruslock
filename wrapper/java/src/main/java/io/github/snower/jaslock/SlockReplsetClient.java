package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackExecutorManager;
import io.github.snower.jaslock.callback.ExecutorOption;
import io.github.snower.jaslock.commands.Command;
import io.github.snower.jaslock.commands.CommandResult;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.PingCommand;
import io.github.snower.jaslock.commands.PingCommandResult;
import io.github.snower.jaslock.exceptions.ClientAsyncCallbackDisabledException;
import io.github.snower.jaslock.exceptions.ClientClosedException;
import io.github.snower.jaslock.exceptions.ClientUnconnectException;
import io.github.snower.jaslock.exceptions.SlockException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Jaslock-compatible replica-set facade backed by one native client handle.
 *
 * <p>The complete node list is handed to Rust at client creation. Rust owns
 * leader discovery, fail-over, retry policy and pending-request tracking, so this
 * class contains no Java child client, no Java request map and no routing
 * algorithm. The protected members are kept for source and binary compatibility
 * only.
 *
 * <p>Construction never loads the native library: the first native operation
 * creates the runtime and the replica-set client.
 */
public class SlockReplsetClient implements ISlockClient {
    private final String[] hosts;
    private final NativeClient nativeClient;
    private final SlockDatabase[] databases = new SlockDatabase[256];
    private final ConcurrentHashMap<SlockClient.BytesKey, Command> requests =
            new ConcurrentHashMap<>();
    private final Object openLock = new Object();
    private final Object closeLock = new Object();
    private final Object callbackLock = new Object();

    private volatile short defaultTimeoutFlag;
    private volatile short defaultExpriedFlag;
    private volatile boolean closed;
    private volatile boolean opened;
    private volatile CallbackExecutorManager callbackExecutorManager;

    public SlockReplsetClient(String hosts) {
        this(hosts.split("\\,"));
    }

    public SlockReplsetClient(String[] hosts) {
        this(hosts, NativeClient.bridgeBackend());
    }

    public SlockReplsetClient(String hosts, boolean enableAsyncCallback) {
        this(hosts.split("\\,"));
        if (enableAsyncCallback) {
            this.enableAsyncCallback();
        }
    }

    public SlockReplsetClient(String[] hosts, boolean enableAsyncCallback) {
        this(hosts);
        if (enableAsyncCallback) {
            this.enableAsyncCallback();
        }
    }

    SlockReplsetClient(String[] hosts, NativeClient.Backend backend) {
        this.hosts = hosts == null ? new String[0] : hosts.clone();
        this.nativeClient = new NativeClient(backend, this.hosts);
    }

    @Override
    public boolean enableAsyncCallback() {
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
    public void open() throws ClientUnconnectException {
        synchronized (openLock) {
            if (opened) {
                return;
            }
            if (closed) {
                throw new ClientUnconnectException("client has been closed");
            }
            try {
                nativeClient.open();
            } catch (IOException e) {
                throw new ClientUnconnectException(message(e));
            } catch (RuntimeException e) {
                throw new ClientUnconnectException(message(e));
            }
            opened = true;
        }
        startAsyncCallbacks();
    }

    @Override
    public ISlockClient tryOpen() {
        boolean connected;
        synchronized (openLock) {
            if (opened) {
                connected = true;
            } else if (closed) {
                connected = false;
            } else {
                try {
                    connected = nativeClient.tryOpen();
                } catch (IOException e) {
                    connected = false;
                } catch (RuntimeException e) {
                    connected = false;
                }
                if (connected) {
                    opened = true;
                }
            }
        }
        if (!connected) {
            return null;
        }
        startAsyncCallbacks();
        return this;
    }

    @Override
    public void close() {
        synchronized (closeLock) {
            if (closed) {
                return;
            }
            closed = true;
        }

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

        nativeClient.close();

        CallbackExecutorManager manager;
        synchronized (callbackLock) {
            manager = callbackExecutorManager;
            callbackExecutorManager = null;
        }
        stopManager(manager);
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

    // ------------------------------------------------------------------
    // Preserved protected compatibility shims
    // ------------------------------------------------------------------

    protected ConcurrentHashMap<SlockClient.BytesKey, Command> getRequests() {
        return requests;
    }

    protected boolean hasLivedClient() {
        return opened && !closed;
    }

    protected void addLivedClient(SlockClient client, boolean isLeader) {
    }

    protected void removeLivedClient(SlockClient client) {
    }

    protected void addLivedLeaderClient(SlockClient client) {
    }

    protected void removeLivedLeaderClient(SlockClient client) {
    }

    protected boolean doPendingRequestCommand(SlockClient client, Command command) {
        return false;
    }

    protected void removePendingRequestCommand(Command command) {
    }

    protected void wakeupPendingRequestCommands(SlockClient client) {
    }

    protected void closePendingRequestCommands() {
    }

    // ------------------------------------------------------------------
    // Package-private facade seams
    // ------------------------------------------------------------------

    NativeClient nativeClient() {
        return nativeClient;
    }

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

    private static String message(Throwable error) {
        String message = error.getMessage();
        return message == null || message.isEmpty() ? error.toString() : message;
    }
}
