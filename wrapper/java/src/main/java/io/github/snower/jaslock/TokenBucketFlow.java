package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Token-bucket flow facade over the dedicated Rust flow handle.
 *
 * <p>Period interpretation, millisecond expiry, retry-after-timeout and
 * priority timeout flag all execute in Rust. Java mirrors the priority byte for
 * the legacy getter and submits one typed acquire per method; the bucket never
 * releases, matching Jaslock.
 *
 * <p>Jaslock creates a fresh inner lock for every token-bucket acquire, so each
 * acquire uses the timeout, count and priority current at that moment. The
 * native handle is created on the first acquire and later timeout/expiry/count
 * setters are forwarded to it. Priority is pushed through its dedicated native
 * operation.
 */
public class TokenBucketFlow extends AbstractExecution {
    private final double period;
    private long nativeHandle;
    private byte priority;

    public TokenBucketFlow(SlockDatabase database, byte[] flowKey, short count, int timeout, double period, byte priority) {
        super(database, flowKey, timeout, 0,
                (short) (count > 0 ? count - 1 : 0), (byte) 0);
        this.period = period;
        this.priority = priority;
    }

    public TokenBucketFlow(SlockDatabase database, byte[] flowKey, short count, int timeout, double period) {
        this(database, flowKey, count, timeout, period, (byte) 0);
    }

    public TokenBucketFlow(SlockDatabase database, String flowKey, short count, int timeout, double period, byte priority) {
        this(database, flowKey.getBytes(StandardCharsets.UTF_8), count, timeout, period, priority);
    }

    public TokenBucketFlow(SlockDatabase database, String flowKey, short count, int timeout, double period) {
        this(database, flowKey.getBytes(StandardCharsets.UTF_8), count, timeout, period, (byte) 0);
    }

    public byte getPriority() {
        return priority;
    }

    public void setPriority(byte priority) {
        long handle;
        synchronized (this) {
            this.priority = priority;
            handle = nativeHandle;
        }
        if (handle != 0) {
            try {
                PrimitiveSupport.submitAndWait(database, handle,
                        NativeBridge.OP_TOKEN_BUCKET_SET_PRIORITY, priority & 0xff, null);
            } catch (SlockException | RuntimeException ignored) {
                // Setters keep their historical non-throwing signature.
            }
        }
    }

    public void acquire() throws SlockException {
        submit(NativeBridge.OP_TOKEN_BUCKET_ACQUIRE, command());
    }

    public CallbackFuture<Boolean> acquire(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_TOKEN_BUCKET_ACQUIRE, command(), result -> {
            try {
                result.getResult();
                callbackFuture.setResult(true);
            } catch (SlockException e) {
                callbackFuture.setResult(false, e);
            }
        });
        return callbackFuture;
    }

    public AutoCloseable with() throws SlockException {
        acquire();
        return () -> {};
    }

    private LockCommand command() {
        return PrimitiveSupport.lockCommand(ICommand.COMMAND_TYPE_LOCK, (byte) 0, database.getDbId(),
                lockKey, new byte[16], timeout, expried, count, (byte) 0, null);
    }

    private void submit(int op, LockCommand command) throws SlockException {
        long handle = ensureNativeHandle();
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, handle, op, 0, null);
        refreshCurrentLockData(handle);
        PrimitiveSupport.throwIfError(result, command);
    }

    private void submitAsync(int op, LockCommand command,
                             Consumer<CallbackCommandResult> callback) throws SlockException {
        long handle = ensureNativeHandle();
        PrimitiveSupport.submitAsync(database, handle, op, 0, null, result -> {
            refreshCurrentLockData(handle);
            callback.accept(PrimitiveSupport.callbackResult(command, result));
        });
    }

    private long ensureNativeHandle() {
        synchronized (this) {
            if (nativeHandle == 0) {
                nativeHandle = NativeBridge.tokenBucketFlowCreate(nativeDatabaseHandle(),
                        lockKey, nativeCount(), timeout, period, priority & 0xff);
            }
            return nativeHandle;
        }
    }

    /**
     * Jaslock creates a fresh inner lock per token-bucket acquire, so packed
     * timeout/expiry changes must reach an already created native handle.
     */
    @Override
    void nativeTimingChanged() {
        long handle;
        synchronized (this) {
            handle = nativeHandle;
        }
        applyNativeTiming(handle);
    }

    /**
     * Jaslock's fresh inner lock uses the current count, so an existing native
     * handle receives the exact encoded wire count.
     */
    @Override
    void nativeConfigurationChanged() {
        long handle;
        synchronized (this) {
            handle = nativeHandle;
        }
        if (handle == 0) {
            return;
        }
        try {
            NativeBridge.primitiveSetCount(handle, Short.toUnsignedInt(count));
        } catch (RuntimeException ignored) {
            // Compatibility setters must not throw; the Java field stays
            // authoritative if the native handle cannot accept the change.
        }
    }

    /** Converts the facade's lock count back to the raw constructor count. */
    private int nativeCount() {
        int current = count;
        if (current < 0) {
            current = 0;
        }
        return current >= 0xffff ? 0xffff : current + 1;
    }
}
