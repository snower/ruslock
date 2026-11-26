package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Max-concurrent flow facade over the dedicated Rust flow handle.
 *
 * <p>The count interpretation, lazy flow identity and priority timeout flag
 * live in Rust. Java mirrors the priority byte for the legacy getter and
 * submits one typed operation per method.
 *
 * <p>The native handle is built on the first operation, matching Jaslock's
 * cached flow initialization. Timeout, expiry, count and priority values
 * current at that moment are captured, and later timing/expiry/count setters
 * intentionally do not reach the cached native flow. Priority changes are still
 * forwarded to the native lazy-priority state, but after the first operation
 * that state no longer changes the cached flow, matching Jaslock.
 */
public class MaxConcurrentFlow extends AbstractExecution {
    private long nativeHandle;
    private byte priority;

    public MaxConcurrentFlow(SlockDatabase database, byte[] flowKey, short count, int timeout, int expried, byte priority) {
        super(database, flowKey, timeout, expried,
                (short) (count > 0 ? count - 1 : 0), (byte) 0);
        this.priority = priority;
    }

    public MaxConcurrentFlow(SlockDatabase database, byte[] flowKey, short count, int timeout, int expried) {
        this(database, flowKey, count, timeout, expried, (byte) 0);
    }

    public MaxConcurrentFlow(SlockDatabase database, String flowKey, short count, int timeout, int expried, byte priority) {
        this(database, flowKey.getBytes(StandardCharsets.UTF_8), count, timeout, expried, priority);
    }

    public MaxConcurrentFlow(SlockDatabase database, String flowKey, short count, int timeout, int expried) {
        this(database, flowKey.getBytes(StandardCharsets.UTF_8), count, timeout, expried);
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
                        NativeBridge.OP_MAX_CONCURRENT_SET_PRIORITY, priority & 0xff, null);
            } catch (SlockException | RuntimeException ignored) {
                // Setters keep their historical non-throwing signature.
            }
        }
    }

    public void acquire() throws SlockException {
        submit(NativeBridge.OP_MAX_CONCURRENT_ACQUIRE, command(ICommand.COMMAND_TYPE_LOCK));
    }

    public CallbackFuture<Boolean> acquire(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_MAX_CONCURRENT_ACQUIRE, command(ICommand.COMMAND_TYPE_LOCK), result -> {
            try {
                result.getResult();
                callbackFuture.setResult(true);
            } catch (SlockException e) {
                callbackFuture.setResult(false, e);
            }
        });
        return callbackFuture;
    }

    public void release() throws SlockException {
        submit(NativeBridge.OP_MAX_CONCURRENT_RELEASE, command(ICommand.COMMAND_TYPE_UNLOCK));
    }

    public CallbackFuture<Boolean> release(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_MAX_CONCURRENT_RELEASE, command(ICommand.COMMAND_TYPE_UNLOCK), result -> {
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
        return this::release;
    }

    private LockCommand command(byte commandType) {
        return PrimitiveSupport.lockCommand(commandType, (byte) 0, database.getDbId(), lockKey,
                new byte[16], timeout, expried, count, priority, null);
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
                nativeHandle = NativeBridge.maxConcurrentFlowCreate(nativeDatabaseHandle(),
                        lockKey, nativeCount(), timeout, expried, priority & 0xff);
            }
            return nativeHandle;
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
