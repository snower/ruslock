package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Priority lock facade over the dedicated Rust priority handle.
 *
 * <p>The priority bit, lazy lock identity and count interpretation live in
 * Rust. Java mirrors only the priority byte for the legacy getter and submits
 * one typed operation per method.
 *
 * <p>The native handle is built on the first operation, matching Jaslock's
 * cached lock initialization. Timeout, count and priority values current at
 * that moment are captured, and later timing/count setters intentionally do not
 * reach the cached native lock. Priority changes are still forwarded to the
 * native lazy-priority state, but after the first operation that state no
 * longer changes the cached lock, matching Jaslock.
 */
public class PriorityLock extends AbstractExecution {
    private long nativeHandle;
    private byte priority;

    public PriorityLock(SlockDatabase database, byte[] lockKey, byte priority, int timeout, int expried, short count) {
        super(database, lockKey, timeout, expried,
                (short) (count > 0 ? count - 1 : 0), (byte) 0);
        this.priority = priority;
    }

    public PriorityLock(SlockDatabase database, byte[] lockKey, byte priority, int timeout, int expried) {
        this(database, lockKey, priority, timeout, expried, (short) 0);
    }

    public PriorityLock(SlockDatabase database, String lockKey, byte priority, int timeout, int expried, short count) {
        this(database, lockKey.getBytes(StandardCharsets.UTF_8), priority, timeout, expried, count);
    }

    public PriorityLock(SlockDatabase database, String lockKey, byte priority, int timeout, int expried) {
        this(database, lockKey.getBytes(StandardCharsets.UTF_8), priority, timeout, expried, (short) 0);
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
                        NativeBridge.OP_PRIORITY_SET_PRIORITY, priority & 0xff, null);
            } catch (SlockException | RuntimeException ignored) {
                // Setters keep their historical non-throwing signature.
            }
        }
    }

    public void acquire() throws SlockException {
        submit(NativeBridge.OP_PRIORITY_ACQUIRE, command(ICommand.COMMAND_TYPE_LOCK));
    }

    public CallbackFuture<Boolean> acquire(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_PRIORITY_ACQUIRE, command(ICommand.COMMAND_TYPE_LOCK), result -> {
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
        submit(NativeBridge.OP_PRIORITY_RELEASE, command(ICommand.COMMAND_TYPE_UNLOCK));
    }

    public CallbackFuture<Boolean> release(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_PRIORITY_RELEASE, command(ICommand.COMMAND_TYPE_UNLOCK), result -> {
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
                nativeHandle = NativeBridge.priorityLockCreate(nativeDatabaseHandle(), lockKey,
                        priority & 0xff, nativeCount(), timeout, expried);
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
