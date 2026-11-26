package io.github.snower.jaslock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Package-private JNI entry points for the Jaslock-compatible facade.
 *
 * <p>This class owns only parameter conversion, opaque handle plumbing, result
 * decoding and the Java-side await adapter. Rust owns all primitive state,
 * connection lifecycle, request correlation and operation execution. There is
 * deliberately no pure-Java fallback.
 *
 * <p>Handles are generation-checked opaque {@code long} tokens. A token may be
 * used only with the factory/lifecycle calls for its kind ({@link #KIND_LOCK},
 * {@link #KIND_EVENT}, ...). Operations are submitted through the single
 * {@link #submit(long, int, int, byte[])} entry; the returned {@link Completion}
 * is completed exactly once by Rust and awaited on the calling Java thread.
 * User callbacks never run on a Rust I/O task.
 *
 * <p>Native symbols live in the {@code ruslock_java} cdylib under
 * {@code wrapper/java/native}.
 */
final class NativeBridge {

    static {
        // Single loading path shared with the loader lane. NativeLibrary fails
        // loudly when this artifact has no library for the running platform.
        NativeLibrary.load();
    }

    private NativeBridge() {
    }

    // Handle kinds.
    static final int KIND_RUNTIME = 1;
    static final int KIND_CLIENT = 2;
    static final int KIND_DATABASE = 3;
    static final int KIND_LOCK = 4;
    static final int KIND_EVENT = 5;
    static final int KIND_COMPLETION = 6;
    static final int KIND_SEMAPHORE = 7;
    static final int KIND_REENTRANT = 8;
    static final int KIND_READ_WRITE = 9;
    static final int KIND_PRIORITY = 10;
    static final int KIND_MAX_CONCURRENT = 11;
    static final int KIND_TOKEN_BUCKET = 12;
    static final int KIND_GROUP_EVENT = 13;
    static final int KIND_TREE = 14;
    static final int KIND_TREE_LEAF = 15;

    // Result status and error kinds. These values are part of the native frame.
    static final int STATUS_OK = 0;
    static final int STATUS_ERROR = 1;

    static final int ERROR_NONE = 0;
    static final int ERROR_CLIENT_CLOSED = 1;
    static final int ERROR_NOT_CONNECTED = 2;
    static final int ERROR_CLIENT_DISCONNECTED = 3;
    static final int ERROR_COMMAND_TIMEOUT = 4;
    static final int ERROR_LOCK_LOCKED = 5;
    static final int ERROR_LOCK_UNLOCKED = 6;
    static final int ERROR_LOCK_NOT_OWN = 7;
    static final int ERROR_LOCK_TIMEOUT = 8;
    static final int ERROR_LOCK_EXPIRED = 9;
    static final int ERROR_STATE = 10;
    static final int ERROR_SERVER = 11;
    static final int ERROR_IO = 12;
    static final int ERROR_PROTOCOL = 13;
    static final int ERROR_LOCK_DATA = 14;
    static final int ERROR_EVENT_WAIT_TIMEOUT = 15;
    static final int ERROR_INVALID_HANDLE = 16;
    static final int ERROR_WRONG_KIND = 17;
    static final int ERROR_STALE_HANDLE = 18;
    static final int ERROR_INVALID_ARGUMENT = 19;
    static final int ERROR_INTERNAL = 20;

    // Single typed operation namespace for Lock and Event handles.
    static final int OP_LOCK_ACQUIRE = 1;
    static final int OP_LOCK_RELEASE = 2;
    static final int OP_LOCK_SHOW = 3;
    static final int OP_LOCK_UPDATE = 4;
    static final int OP_LOCK_RELEASE_HEAD = 5;
    static final int OP_LOCK_RELEASE_HEAD_TO_LOCK_WAIT = 6;
    static final int OP_LOCK_SET_COUNT = 7;
    static final int OP_LOCK_SET_R_COUNT = 8;

    static final int OP_EVENT_IS_SET = 20;
    static final int OP_EVENT_SET = 21;
    /**
     * Clears the event. {@code data} is optional: {@code null} performs a plain
     * clear, non-null performs the Java facade's data-carrying clear.
     */
    static final int OP_EVENT_CLEAR = 22;
    /** Waits with packed timeout bits in {@code arg0}. */
    static final int OP_EVENT_WAIT = 23;
    /**
     * Waits with packed timeout bits in {@code arg0}, then applies the Java
     * facade's timeout retry-clear compensation when the wait times out.
     */
    static final int OP_EVENT_WAIT_RETRY_CLEAR = 24;

    static final int OP_SEMAPHORE_ACQUIRE = 30;
    static final int OP_SEMAPHORE_RELEASE = 31;

    static final int OP_REENTRANT_ACQUIRE = 32;
    static final int OP_REENTRANT_RELEASE = 33;

    static final int OP_READ_WRITE_ACQUIRE_WRITE = 34;
    static final int OP_READ_WRITE_RELEASE_WRITE = 35;
    static final int OP_READ_WRITE_ACQUIRE_READ = 36;
    static final int OP_READ_WRITE_RELEASE_READ = 37;

    static final int OP_GROUP_EVENT_IS_SET = 40;
    static final int OP_GROUP_EVENT_SET = 41;
    static final int OP_GROUP_EVENT_CLEAR = 42;
    static final int OP_GROUP_EVENT_WAKEUP = 43;
    static final int OP_GROUP_EVENT_WAIT = 44;
    static final int OP_GROUP_EVENT_WAIT_RETRY_CLEAR = 45;
    static final int OP_GROUP_EVENT_WAKEUP_WITH_DATA = 46;

    static final int OP_PRIORITY_ACQUIRE = 50;
    static final int OP_PRIORITY_RELEASE = 51;
    static final int OP_PRIORITY_SET_PRIORITY = 52;

    static final int OP_MAX_CONCURRENT_ACQUIRE = 53;
    static final int OP_MAX_CONCURRENT_RELEASE = 54;
    static final int OP_MAX_CONCURRENT_SET_PRIORITY = 55;

    static final int OP_TOKEN_BUCKET_ACQUIRE = 56;
    static final int OP_TOKEN_BUCKET_SET_PRIORITY = 57;

    static final int OP_TREE_ACQUIRE = 60;
    static final int OP_TREE_RELEASE = 61;
    static final int OP_TREE_WAIT = 62;

    static final int OP_TREE_LEAF_ACQUIRE = 63;
    static final int OP_TREE_LEAF_RELEASE = 64;

    // ------------------------------------------------------------------
    // Runtime and client lifecycle
    // ------------------------------------------------------------------

    /** Creates a logical runtime handle backed by the process-wide Tokio runtime. */
    static native long runtimeCreate();

    /** Releases a runtime handle. Clients keep the shared runtime alive. */
    static void runtimeDestroy(long runtime) {
        release(runtime);
    }

    /**
     * Creates a client from one or more node endpoints. A single node uses the
     * single-node Rust client; multiple nodes use the replica-set client. Java
     * never routes between nodes itself. The client starts unopened and unclosed.
     */
    static native long clientCreateNodes(long runtime, String[] nodes);

    /** Strict open; throws {@link IOException} when the first handshake fails. */
    static native void clientOpen(long client) throws IOException;

    /**
     * Opens without turning an initial refusal into an error.
     *
     * @return {@code 1} when this call completed the handshake, {@code 0} when a
     *         managed background reconnect was started instead.
     */
    static native int clientTryOpen(long client) throws IOException;

    /** Idempotently closes the client and completes pending requests with errors. */
    static native void clientClose(long client);

    /** Returns whether an explicit close has been applied to this handle. */
    static native boolean clientIsClosed(long client);

    // ------------------------------------------------------------------
    // Database, Lock and Event construction / metadata
    // ------------------------------------------------------------------

    static native long databaseCreate(long client, int dbId);

    /** Sets the 16-bit default timeout/expired flag halves merged by Rust. */
    static native void databaseSetDefaultFlags(long database, int timeoutFlags, int expiredFlags);

    /**
     * Creates a lock with full packed timeout/expired bit patterns.
     *
     * @param lockId {@code null} for a generated id, otherwise exactly 16 bytes.
     */
    static native long lockCreate(long database, byte[] key, int timeoutBits, int expiredBits,
                                  byte[] lockId);

    /** Creates an event; {@code defaultSet} selects the event's set/clear polarity. */
    static native long eventCreate(long database, byte[] key, int timeoutBits, int expiredBits,
                                   boolean defaultSet);

    // ------------------------------------------------------------------
    // Extended primitive constructors
    // ------------------------------------------------------------------

    /**
     * Creates a semaphore. {@code count} is the caller's raw count; Rust stores
     * {@code count - 1} exactly like the Java facade.
     */
    static native long semaphoreCreate(long database, byte[] key, int count, int timeoutBits,
                                       int expiredBits);

    /** Creates a reentrant lock with a generated lock id. */
    static native long reentrantLockCreate(long database, byte[] key, int timeoutBits,
                                           int expiredBits);

    /** Creates a read-write lock with a generated write lock id and empty read queue. */
    static native long readWriteLockCreate(long database, byte[] key, int timeoutBits,
                                           int expiredBits);

    /** Creates a priority lock. {@code priority} is the initial priority byte. */
    static native long priorityLockCreate(long database, byte[] key, int priority, int count,
                                          int timeoutBits, int expiredBits);

    /** Creates a max-concurrent flow. {@code count} is the maximum concurrency. */
    static native long maxConcurrentFlowCreate(long database, byte[] key, int count,
                                               int timeoutBits, int expiredBits, int priority);

    /**
     * Creates a token-bucket flow.
     *
     * @param count  bucket capacity
     * @param period refill period in seconds
     */
    static native long tokenBucketFlowCreate(long database, byte[] key, int count, int timeoutBits,
                                             double period, int priority);

    /**
     * Creates a group event with the caller's client id and current version.
     *
     * <p>{@code clientId} is the client's non-zero id; {@code versionId} is the
     * last version observed by the caller, or {@code 0} for a fresh handle.
     */
    static native long groupEventCreate(long database, byte[] key, long clientId, long versionId,
                                        int timeoutBits, int expiredBits);

    /**
     * Replaces the native packed timeout/expired bits on a live primitive.
     *
     * <p>The bits are stored exactly as supplied; the Rust binding assigns them
     * verbatim and does not re-merge database defaults or clear high flag bits.
     * Primitive state (count, ids, read queue, version, lazy priority) is
     * preserved; the object is never recreated. Applies to lock, event,
     * semaphore, reentrant, read-write, priority, max-concurrent flow,
     * token-bucket flow, group event, tree and tree leaf handles, and is safe
     * before the first operation.
     *
     * <p>Handle kinds without timing state (client, database, runtime,
     * completion) throw {@link IllegalArgumentException}; a released handle
     * throws {@link IllegalStateException}.
     */
    static native void primitiveSetTiming(long handle, int timeoutBits, int expiredBits);

    /**
     * Replaces the stored wire count on a counting primitive.
     *
     * <p>Applies to semaphore and token-bucket flow handles only. {@code count}
     * is the facade's already-encoded count (for example
     * {@code Short.toUnsignedInt(count)}); it is passed to native verbatim with
     * no offset. Timing, period and priority are preserved; the object is never
     * recreated.
     *
     * @throws IllegalArgumentException if {@code count} is outside
     *                                  {@code 0..65535}, or the handle kind has
     *                                  no wire count
     * @throws IllegalStateException    if the handle is invalid or released
     */
    static native void primitiveSetCount(long handle, int count);

    /**
     * Returns the group event's current (in-memory) version id.
     */
    static native long groupEventVersionId(long groupEvent);

    /**
     * Creates a tree lock.
     *
     * @param parentKey {@code null} for a root node, otherwise exactly 16 bytes
     * @param key       the node key
     */
    static native long treeLockCreate(long database, byte[] parentKey, byte[] key, int timeoutBits,
                                      int expiredBits);

    /**
     * Creates a leaf lock bound to a tree.
     *
     * @param lockId {@code null} to generate a new leaf id, otherwise exactly 16
     *               bytes identifying an existing leaf
     */
    static native long treeLeafCreate(long tree, byte[] lockId);

    /** Returns the versioned metadata frame; see {@link Metadata#parse(byte[])}. */
    private static native byte[] metadataNative(long handle);

    static Metadata metadata(long handle) {
        return Metadata.parse(metadataNative(handle));
    }

    /** Releases any handle. Idempotent; releasing a client also closes it. */
    static native boolean release(long handle);

    // ------------------------------------------------------------------
    // Operations
    // ------------------------------------------------------------------

    private static native long submitNative(long target, int op, int arg0, byte[] data);

    /**
     * Submits one typed operation and returns an unawaited completion handle.
     *
     * <p>{@code data} is {@code LockData.dumpData()} bytes or {@code null} when
     * the operation carries no data. For lock ops {@code arg0} is the command
     * flag byte; for {@code OP_LOCK_SET_COUNT}/{@code OP_LOCK_SET_R_COUNT} it is
     * the count; for {@code OP_EVENT_WAIT} and
     * {@code OP_EVENT_WAIT_RETRY_CLEAR} it is the caller's full packed timeout
     * bits. {@code OP_EVENT_CLEAR} accepts optional data. The completion must be
     * awaited and/or released by the caller.
     */
    static Completion submit(long target, int op, int arg0, byte[] data) {
        long token = submitNative(target, op, arg0, data);
        if (token == 0) {
            throw new IllegalStateException("native operation submission returned no completion");
        }
        return new Completion(token);
    }

    /** Synchronous adapter used by Java sync facade methods. */
    static Result submitAndWait(long target, int op, int arg0, byte[] data) {
        Completion completion = submit(target, op, arg0, data);
        try {
            return completion.await();
        } finally {
            completion.close();
        }
    }

    static native boolean completionIsDone(long completion);

    private static native byte[] completionAwait(long completion);

    /**
     * Returns the process-wide completion generation. This is non-blocking and
     * safe on any thread.
     *
     * <p>A pump should read this value before scanning its own outstanding
     * completions, then call {@link #awaitCompletionChange(long, long)} only if
     * its scan found nothing ready.
     */
    static native long completionEpoch();

    /**
     * Waits until the completion generation differs from {@code observed}.
     *
     * <p>{@code timeoutMillis < 0} waits indefinitely, {@code 0} returns
     * immediately, and a positive value bounds the wait. Returns the current
     * generation, which may still equal {@code observed} after a timeout. Must
     * be called only on the dedicated completion pump thread, never on a user
     * callback thread.
     */
    static native long awaitCompletionChange(long observed, long timeoutMillis);

    /**
     * Bumps the completion generation without a native result.
     *
     * <p>Call after adding a token to the Java outstanding queue so a pump that
     * already scanned cannot miss the new token. This is a pure wake-up signal;
     * it creates no native records and transfers no ownership.
     */
    static native void signalCompletionPump();

    // ------------------------------------------------------------------
    // Low-level command transport (sendCommand / writeCommand)
    // ------------------------------------------------------------------

    /**
     * Submits a complete caller-supplied frame (64-byte header plus extra bytes)
     * and returns an opaque completion token. The completion resolves through
     * {@link #sendRawAwait(long)} to the exact response frame in the Jaslock
     * {@code Command} layout.
     *
     * <p>{@code timeoutMillis} is the caller's total wait deadline, already
     * including the Jaslock command window. Native asks the core transport for
     * an exact response deadline with no extra grace and never cancels the
     * frame write.
     */
    private static native long sendRawSubmitNative(long client, byte[] frame, long timeoutMillis)
            throws IOException;

    static long sendRawSubmit(long client, byte[] frame, long timeoutMillis) throws IOException {
        long token = sendRawSubmitNative(client, frame, timeoutMillis);
        if (token == 0) {
            throw new IllegalStateException("native raw send returned no completion");
        }
        return token;
    }

    /**
     * Submits a complete caller-supplied frame without waiting for, or
     * fabricating, a response. The completion resolves to an empty payload once
     * the frame has been written and flushed.
     */
    private static native long writeRawSubmitNative(long client, byte[] frame) throws IOException;

    static long writeRawSubmit(long client, byte[] frame) throws IOException {
        long token = writeRawSubmitNative(client, frame);
        if (token == 0) {
            throw new IllegalStateException("native raw write returned no completion");
        }
        return token;
    }

    /**
     * Blocks the calling Java thread until a raw submit completes. Success
     * returns the response frame bytes (empty for a write-only submit); transport
     * failures throw {@link IOException}. The completion token is not consumed
     * and must be released with {@link #release(long)}.
     */
    static native byte[] sendRawAwait(long completion) throws IOException;

    /** Synchronous convenience for {@link #sendRawSubmit} + {@link #sendRawAwait}. */
    static byte[] sendCommand(long client, byte[] frame, long timeoutMillis) throws IOException {
        long completion = sendRawSubmit(client, frame, timeoutMillis);
        try {
            return sendRawAwait(completion);
        } finally {
            release(completion);
        }
    }

    /** Synchronous convenience for {@link #writeRawSubmit} + {@link #sendRawAwait}. */
    static void writeCommand(long client, byte[] frame) throws IOException {
        long completion = writeRawSubmit(client, frame);
        try {
            sendRawAwait(completion);
        } finally {
            release(completion);
        }
    }

    // ------------------------------------------------------------------
    // Completion and result decoding
    // ------------------------------------------------------------------

    static final class Completion {
        private final long token;
        private boolean released;

        private Completion(long token) {
            this.token = token;
        }

        long token() {
            return token;
        }

        boolean isDone() {
            return completionIsDone(token);
        }

        /**
         * Blocks the calling Java thread until Rust completes the operation, then
         * returns the structured result. The completion remains valid until
         * {@link #close()}; callers using {@link #submitAndWait} do not close it.
         */
        Result await() {
            if (released) {
                throw new IllegalStateException("completion has already been released");
            }
            return Result.parse(completionAwait(token));
        }

        void close() {
            if (!released) {
                released = true;
                release(token);
            }
        }
    }

    static final class Result {
        final int status;
        final int errorKind;
        final int serverResult;
        final boolean hasValue;
        final boolean value;
        final LockResult lock;
        final byte[] data;
        final String message;

        private Result(int status, int errorKind, int serverResult, boolean hasValue,
                       boolean value, LockResult lock, byte[] data, String message) {
            this.status = status;
            this.errorKind = errorKind;
            this.serverResult = serverResult;
            this.hasValue = hasValue;
            this.value = value;
            this.lock = lock;
            this.data = data;
            this.message = message;
        }

        boolean isOk() {
            return status == STATUS_OK;
        }

        static Result parse(byte[] encoded) {
            if (encoded == null) {
                throw new IllegalStateException("native completion returned no result frame");
            }
            ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
            int version = buffer.get() & 0xff;
            if (version != 1) {
                throw new IllegalStateException("unsupported native result version " + version);
            }
            int status = buffer.get() & 0xff;
            int errorKind = buffer.get() & 0xff;
            int flags = buffer.get() & 0xff;
            int rawServerResult = buffer.get() & 0xff;
            buffer.get();
            buffer.getShort();

            int messageLength = buffer.getInt();
            String message = new String(read(buffer, messageLength), StandardCharsets.UTF_8);

            byte[] bytes = null;
            if ((flags & 16) != 0) {
                bytes = read(buffer, buffer.getInt());
            }

            LockResult lock = null;
            if ((flags & 1) != 0) {
                int commandType = buffer.get() & 0xff;
                int resultCode = buffer.get() & 0xff;
                int flag = buffer.get() & 0xff;
                int dbId = buffer.get() & 0xff;
                int lrCount = buffer.get() & 0xff;
                int rCount = buffer.get() & 0xff;
                int hasData = buffer.get() & 0xff;
                buffer.get();
                int lCount = buffer.getShort() & 0xffff;
                int count = buffer.getShort() & 0xffff;
                int dataLength = buffer.getInt();
                byte[] requestId = read(buffer, 16);
                byte[] lockId = read(buffer, 16);
                byte[] lockKey = read(buffer, 16);
                byte[] data = hasData != 0 ? read(buffer, dataLength) : null;
                lock = new LockResult(commandType, resultCode, flag, dbId, lrCount, rCount,
                        lCount, count, requestId, lockId, lockKey, data);
            }

            int serverResult = (flags & 8) != 0 ? rawServerResult : -1;
            boolean hasValue = (flags & 2) != 0;
            boolean value = (flags & 4) != 0;
            return new Result(status, errorKind, serverResult, hasValue, value, lock, bytes,
                    message);
        }

        private static byte[] read(ByteBuffer buffer, int length) {
            if (length < 0 || length > buffer.remaining()) {
                throw new IllegalStateException("native result frame is truncated");
            }
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return bytes;
        }
    }

    static final class LockResult {
        final int commandType;
        final int result;
        final int flag;
        final int dbId;
        final int lrCount;
        final int rCount;
        final int lCount;
        final int count;
        final byte[] requestId;
        final byte[] lockId;
        final byte[] lockKey;
        final byte[] data;

        private LockResult(int commandType, int result, int flag, int dbId, int lrCount,
                           int rCount, int lCount, int count, byte[] requestId, byte[] lockId,
                           byte[] lockKey, byte[] data) {
            this.commandType = commandType;
            this.result = result;
            this.flag = flag;
            this.dbId = dbId;
            this.lrCount = lrCount;
            this.rCount = rCount;
            this.lCount = lCount;
            this.count = count;
            this.requestId = requestId;
            this.lockId = lockId;
            this.lockKey = lockKey;
            this.data = data;
        }
    }

    static final class Metadata {
        final int kind;
        final int flagA;
        final int dbId;
        final long timeoutBits;
        final long expiredBits;
        final int count;
        final int rCount;
        final int defaultTimeoutFlags;
        final int defaultExpiredFlags;
        final byte[] key;
        final byte[] lockId;
        final byte[] currentData;
        final int priority;
        final int max;
        final double period;
        final long clientId;
        final long versionId;
        final int readLockCount;
        final byte[] parentKey;
        final byte[] leafId;

        private Metadata(int kind, int flagA, int dbId, long timeoutBits, long expiredBits,
                         int count, int rCount, int defaultTimeoutFlags,
                         int defaultExpiredFlags, byte[] key, byte[] lockId, byte[] currentData,
                         int priority, int max, double period, long clientId, long versionId,
                         int readLockCount, byte[] parentKey, byte[] leafId) {
            this.kind = kind;
            this.flagA = flagA;
            this.dbId = dbId;
            this.timeoutBits = timeoutBits;
            this.expiredBits = expiredBits;
            this.count = count;
            this.rCount = rCount;
            this.defaultTimeoutFlags = defaultTimeoutFlags;
            this.defaultExpiredFlags = defaultExpiredFlags;
            this.key = key;
            this.lockId = lockId;
            this.currentData = currentData;
            this.priority = priority;
            this.max = max;
            this.period = period;
            this.clientId = clientId;
            this.versionId = versionId;
            this.readLockCount = readLockCount;
            this.parentKey = parentKey;
            this.leafId = leafId;
        }

        /** For client returns the closed flag, for event defaultSet, for completion done. */
        boolean flag() {
            return flagA != 0;
        }

        static Metadata parse(byte[] encoded) {
            if (encoded == null) {
                throw new IllegalStateException("native metadata returned no frame");
            }
            ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
            int version = buffer.get() & 0xff;
            if (version != 1) {
                throw new IllegalStateException("unsupported native metadata version " + version);
            }
            int kind = buffer.get() & 0xff;
            int flagA = buffer.get() & 0xff;
            int dbId = buffer.get() & 0xff;
            long timeoutBits = buffer.getInt() & 0xffffffffL;
            long expiredBits = buffer.getInt() & 0xffffffffL;
            int count = buffer.getShort() & 0xffff;
            int rCount = buffer.get() & 0xff;
            buffer.get();
            int defaultTimeoutFlags = buffer.getShort() & 0xffff;
            int defaultExpiredFlags = buffer.getShort() & 0xffff;

            byte[] key = null;
            if ((buffer.get() & 0xff) != 0) {
                key = read(buffer, 16);
            }
            byte[] lockId = null;
            if ((buffer.get() & 0xff) != 0) {
                lockId = read(buffer, 16);
            }
            byte[] currentData = null;
            if ((buffer.get() & 0xff) != 0) {
                currentData = read(buffer, buffer.getInt());
            } else {
                buffer.getInt();
            }
            int priority = 0;
            int max = 0;
            double period = 0.0d;
            long clientId = 0L;
            long versionId = 0L;
            int readLockCount = 0;
            byte[] parentKey = null;
            byte[] leafId = null;
            if (buffer.remaining() >= 1) {
                int extVersion = buffer.get() & 0xff;
                if (extVersion == 1) {
                    if (buffer.remaining() < 25) {
                        throw new IllegalStateException("native metadata extension is truncated");
                    }
                    priority = buffer.get() & 0xff;
                    max = buffer.getShort() & 0xffff;
                    period = buffer.getDouble();
                    clientId = buffer.getLong();
                    versionId = buffer.getLong();
                    readLockCount = buffer.getInt();
                    if ((buffer.get() & 0xff) != 0) {
                        parentKey = read(buffer, 16);
                    }
                    if ((buffer.get() & 0xff) != 0) {
                        leafId = read(buffer, 16);
                    }
                }
            }
            return new Metadata(kind, flagA, dbId, timeoutBits, expiredBits, count, rCount,
                    defaultTimeoutFlags, defaultExpiredFlags, key, lockId, currentData,
                    priority, max, period, clientId, versionId, readLockCount, parentKey, leafId);
        }

        private static byte[] read(ByteBuffer buffer, int length) {
            if (length < 0 || length > buffer.remaining()) {
                throw new IllegalStateException("native metadata frame is truncated");
            }
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return bytes;
        }
    }
}
