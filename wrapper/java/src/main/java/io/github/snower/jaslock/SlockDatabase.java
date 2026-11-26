package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackExecutorManager;
import io.github.snower.jaslock.exceptions.ClientAsyncCallbackDisabledException;
import io.github.snower.jaslock.exceptions.ClientClosedException;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;

/**
 * Jaslock-compatible database facade.
 *
 * <p>The database keeps only the public Jaslock surface and the legacy default
 * flag merging. The opaque native database handle is created lazily from the
 * owning client handle, so constructing a database never loads the native
 * library and never opens a connection.
 */
public class SlockDatabase {
    private ISlockClient client;
    private final byte dbId;
    private short defaultTimeoutFlag;
    private short defaultExpriedFlag;
    private long nativeHandle;

    public SlockDatabase(ISlockClient client, byte dbId, short defaultTimeoutFlag, short defaultExpriedFlag) {
        this.client = client;
        this.dbId = dbId;
        this.defaultTimeoutFlag = defaultTimeoutFlag;
        this.defaultExpriedFlag = defaultExpriedFlag;
    }

    public void setDefaultTimeoutFlag(short defaultTimeoutFlag) {
        NativeClient nativeClient;
        long handle;
        short expired;
        synchronized (this) {
            this.defaultTimeoutFlag = defaultTimeoutFlag;
            nativeClient = nativeClientIfAvailable();
            handle = nativeHandle;
            expired = defaultExpriedFlag;
        }
        pushDefaultFlags(nativeClient, handle, defaultTimeoutFlag, expired);
    }

    public void setDefaultExpriedFlag(short defaultExpriedFlag) {
        NativeClient nativeClient;
        long handle;
        short timeout;
        synchronized (this) {
            this.defaultExpriedFlag = defaultExpriedFlag;
            nativeClient = nativeClientIfAvailable();
            handle = nativeHandle;
            timeout = defaultTimeoutFlag;
        }
        pushDefaultFlags(nativeClient, handle, timeout, defaultExpriedFlag);
    }

    public void close() {
        NativeClient nativeClient;
        long handle;
        synchronized (this) {
            if (client == null) {
                return;
            }
            nativeClient = nativeClientIfAvailable();
            handle = nativeHandle;
            nativeHandle = 0;
            client = null;
        }
        if (nativeClient != null && handle != 0) {
            try {
                nativeClient.release(handle);
            } catch (RuntimeException ignored) {
                // Closing is idempotent; a stale handle is already unusable.
            }
        }
    }

    public ISlockClient getClient() throws ClientClosedException {
        ISlockClient current;
        synchronized (this) {
            current = client;
        }
        if (current == null) {
            throw new ClientClosedException("database close");
        }
        return current;
    }

    public byte getDbId() {
        return dbId;
    }

    // ------------------------------------------------------------------
    // Package-private coordination used by the primitive and client lanes
    // ------------------------------------------------------------------

    /**
     * Returns the opaque native database handle, creating it on first use.
     *
     * <p>This entry is intentionally unchecked: primitive constructors and
     * {@link PrimitiveSupport} call it from signatures that cannot add a checked
     * exception. A closed database surfaces the same state as a closed native
     * handle.
     */
    long nativeHandle() {
        synchronized (this) {
            if (client == null) {
                throw new IllegalStateException("database close");
            }
            if (nativeHandle != 0) {
                return nativeHandle;
            }
            NativeClient nativeClient = nativeClientIfAvailable();
            if (nativeClient == null) {
                throw new IllegalStateException(
                        "database is not backed by a native client");
            }
            nativeHandle = nativeClient
                    .createDatabase(dbId, defaultTimeoutFlag, defaultExpriedFlag);
            return nativeHandle;
        }
    }

    /** Requires a configured, running asynchronous callback dispatcher. */
    void requireAsyncCallback() throws SlockException {
        NativeClient.requireExecutor(requireManager());
    }

    /** Runs a native completion task on the configured callback dispatcher. */
    void dispatch(Runnable task) throws SlockException {
        NativeClient.dispatch(requireManager(), task);
    }

    private CallbackExecutorManager requireManager() throws SlockException {
        getClient();
        CallbackExecutorManager manager = managerIfAvailable();
        if (manager == null) {
            throw new ClientAsyncCallbackDisabledException(
                    "The asynchronous thread pool is not enabled. First enableAsyncCallback"
                            + " to enable the asynchronous thread pool.");
        }
        return manager;
    }

    /**
     * Resolves the native client only for the two concrete facade classes. A
     * caller-supplied {@link ISlockClient} is supported for the compatibility
     * surface but has no native backend, so native primitives report that
     * condition instead of silently using a Java algorithm.
     */
    private NativeClient nativeClientIfAvailable() {
        ISlockClient current = client;
        if (current instanceof SlockClient) {
            return ((SlockClient) current).nativeClient();
        }
        if (current instanceof SlockReplsetClient) {
            return ((SlockReplsetClient) current).nativeClient();
        }
        return null;
    }

    private CallbackExecutorManager managerIfAvailable() {
        ISlockClient current = client;
        if (current instanceof SlockClient) {
            return ((SlockClient) current).asyncCallbackManager();
        }
        if (current instanceof SlockReplsetClient) {
            return ((SlockReplsetClient) current).asyncCallbackManager();
        }
        return null;
    }

    private static void pushDefaultFlags(NativeClient nativeClient, long handle,
                                         short timeoutFlag, short expriedFlag) {
        if (nativeClient == null || handle == 0) {
            return;
        }
        try {
            nativeClient.databaseDefaultFlagsChanged(handle, timeoutFlag, expriedFlag);
        } catch (RuntimeException ignored) {
            // Setters keep their historical non-throwing signature.
        }
    }

    private int mergeTimeoutFlag(int timeout) {
        if (defaultTimeoutFlag != 0) {
            timeout = timeout | ((((int) defaultTimeoutFlag) & 0xffff) << 16);
        }
        return timeout;
    }

    private int mergeExpriedFlag(int expried) {
        if (defaultExpriedFlag != 0) {
            expried = expried | ((((int) defaultExpriedFlag) & 0xffff) << 16);
        }
        return expried;
    }

    public Lock newLock(byte[] lockKey, int timeout, int expried) {
        return new Lock(this, lockKey, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public Lock newLock(String lockKey, int timeout, int expried) {
        return new Lock(this, lockKey.getBytes(StandardCharsets.UTF_8), mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public Event newEvent(byte[] eventKey, int timeout, int expried, boolean defaultSeted) {
        return new Event(this, eventKey, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried), defaultSeted);
    }

    public Event newEvent(String eventKey, int timeout, int expried, boolean defaultSeted) {
        return new Event(this, eventKey.getBytes(StandardCharsets.UTF_8), mergeTimeoutFlag(timeout), mergeExpriedFlag(expried), defaultSeted);
    }

    public ReentrantLock newReentrantLock(byte[] lockKey, int timeout, int expried) {
        return new ReentrantLock(this, lockKey, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public ReentrantLock newReentrantLock(String lockKey, int timeout, int expried) {
        return new ReentrantLock(this, lockKey.getBytes(StandardCharsets.UTF_8), mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public ReadWriteLock newReadWriteLock(byte[] lockKey, int timeout, int expried) {
        return new ReadWriteLock(this, lockKey, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public ReadWriteLock newReadWriteLock(String lockKey, int timeout, int expried) {
        return new ReadWriteLock(this, lockKey.getBytes(StandardCharsets.UTF_8), mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public Semaphore newSemaphore(byte[] semaphoreKey, short count, int timeout, int expried) {
        return new Semaphore(this, semaphoreKey, count, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public Semaphore newSemaphore(String semaphoreKey, short count, int timeout, int expried) {
        return new Semaphore(this, semaphoreKey.getBytes(StandardCharsets.UTF_8), count, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public MaxConcurrentFlow newMaxConcurrentFlow(byte[] flowKey, short count, int timeout, int expried) {
        return new MaxConcurrentFlow(this, flowKey, count, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public MaxConcurrentFlow newMaxConcurrentFlow(String flowKey, short count, int timeout, int expried) {
        return new MaxConcurrentFlow(this, flowKey.getBytes(StandardCharsets.UTF_8), count, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public MaxConcurrentFlow newMaxConcurrentFlow(byte[] flowKey, short count, int timeout, int expried, byte priority) {
        return new MaxConcurrentFlow(this, flowKey, count, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried), priority);
    }

    public MaxConcurrentFlow newMaxConcurrentFlow(String flowKey, short count, int timeout, int expried, byte priority) {
        return new MaxConcurrentFlow(this, flowKey.getBytes(StandardCharsets.UTF_8), count, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried), priority);
    }

    public TokenBucketFlow newTokenBucketFlow(byte[] flowKey, short count, int timeout, double period) {
        return new TokenBucketFlow(this, flowKey, count, mergeTimeoutFlag(timeout), period);
    }

    public TokenBucketFlow newTokenBucketFlow(String flowKey, short count, int timeout, double period) {
        return new TokenBucketFlow(this, flowKey.getBytes(StandardCharsets.UTF_8), count, mergeTimeoutFlag(timeout), period);
    }

    public TokenBucketFlow newTokenBucketFlow(byte[] flowKey, short count, int timeout, double period, byte priority) {
        return new TokenBucketFlow(this, flowKey, count, mergeTimeoutFlag(timeout), period, priority);
    }

    public TokenBucketFlow newTokenBucketFlow(String flowKey, short count, int timeout, double period, byte priority) {
        return new TokenBucketFlow(this, flowKey.getBytes(StandardCharsets.UTF_8), count, mergeTimeoutFlag(timeout), period, priority);
    }

    public GroupEvent newGroupEvent(byte[] groupKey, long clientId, long versionId, int timeout, int expried) {
        return new GroupEvent(this, groupKey, clientId, versionId, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public GroupEvent newGroupEvent(String groupKey, long clientId, long versionId, int timeout, int expried) {
        return new GroupEvent(this, groupKey, clientId, versionId, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public TreeLock newTreeLock(byte[] parentKey, byte[] lockKey, int timeout, int expried) {
        return new TreeLock(this, parentKey, lockKey, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public TreeLock newTreeLock(String parentKey, String lockKey, int timeout, int expried) {
        return new TreeLock(this, parentKey, lockKey, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public TreeLock newTreeLock(byte[] lockKey, int timeout, int expried) {
        return new TreeLock(this, lockKey, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public TreeLock newTreeLock(String lockKey, int timeout, int expried) {
        return new TreeLock(this, lockKey, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public PriorityLock newPriorityLock(byte[] lockKey, byte priority, int timeout, int expried) {
        return new PriorityLock(this, lockKey, priority, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }

    public PriorityLock newPriorityLock(String lockKey, byte priority, int timeout, int expried) {
        return new PriorityLock(this, lockKey, priority, mergeTimeoutFlag(timeout), mergeExpriedFlag(expried));
    }
}
