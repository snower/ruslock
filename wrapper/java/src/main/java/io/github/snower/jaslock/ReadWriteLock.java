package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Read-write lock facade over the dedicated Rust read-write handle.
 *
 * <p>The write lock identity and the read-lock FIFO queue live in Rust; Java
 * only submits one typed operation per method.
 *
 * <p>The native handle is built on the first operation, matching Jaslock's
 * cached write-lock initialization. Timeout and expiry setters are forwarded to
 * the handle; the Rust binding keeps the cached write-lock timing unchanged and
 * applies the updated packed timing to the fresh read lock that Jaslock builds
 * per {@code acquireRead}. This reproduces Jaslock's cached-write/fresh-read
 * lifetime without any timing or FIFO logic in Java.
 */
public class ReadWriteLock extends AbstractExecution {
    private long nativeHandle;

    public ReadWriteLock(SlockDatabase database, byte[] lockKey, int timeout, int expried) {
        super(database, lockKey, timeout, expried);
    }

    public ReadWriteLock(SlockDatabase database, String lockKey, int timeout, int expried) {
        this(database, lockKey.getBytes(StandardCharsets.UTF_8), timeout, expried);
    }

    public void acquireWrite() throws SlockException {
        submit(NativeBridge.OP_READ_WRITE_ACQUIRE_WRITE, command(ICommand.COMMAND_TYPE_LOCK));
    }

    public CallbackFuture<Boolean> acquireWrite(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_READ_WRITE_ACQUIRE_WRITE, command(ICommand.COMMAND_TYPE_LOCK), result -> {
            try {
                result.getResult();
                callbackFuture.setResult(true);
            } catch (SlockException e) {
                callbackFuture.setResult(false, e);
            }
        });
        return callbackFuture;
    }

    public void releaseWrite() throws SlockException {
        submit(NativeBridge.OP_READ_WRITE_RELEASE_WRITE, command(ICommand.COMMAND_TYPE_UNLOCK));
    }

    public CallbackFuture<Boolean> releaseWrite(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_READ_WRITE_RELEASE_WRITE, command(ICommand.COMMAND_TYPE_UNLOCK), result -> {
            try {
                result.getResult();
                callbackFuture.setResult(true);
            } catch (SlockException e) {
                callbackFuture.setResult(false, e);
            }
        });
        return callbackFuture;
    }

    public void acquireRead() throws SlockException {
        submit(NativeBridge.OP_READ_WRITE_ACQUIRE_READ, command(ICommand.COMMAND_TYPE_LOCK));
    }

    public CallbackFuture<Boolean> acquireRead(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_READ_WRITE_ACQUIRE_READ, command(ICommand.COMMAND_TYPE_LOCK), result -> {
            try {
                result.getResult();
                callbackFuture.setResult(true);
            } catch (SlockException e) {
                callbackFuture.setResult(false, e);
            }
        });
        return callbackFuture;
    }

    public void releaseRead() throws SlockException {
        submit(NativeBridge.OP_READ_WRITE_RELEASE_READ, command(ICommand.COMMAND_TYPE_UNLOCK));
    }

    public CallbackFuture<Boolean> releaseRead(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_READ_WRITE_RELEASE_READ, command(ICommand.COMMAND_TYPE_UNLOCK), result -> {
            try {
                result.getResult();
                callbackFuture.setResult(true);
            } catch (SlockException e) {
                callbackFuture.setResult(false, e);
            }
        });
        return callbackFuture;
    }

    public void acquire() throws SlockException {
        acquireWrite();
    }

    public CallbackFuture<Boolean> acquire(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return acquireWrite(callback);
    }

    public void release() throws SlockException {
        releaseWrite();
    }

    public CallbackFuture<Boolean> release(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return releaseWrite(callback);
    }

    public AutoCloseable with() throws SlockException {
        acquire();
        return this::release;
    }

    private LockCommand command(byte commandType) {
        return PrimitiveSupport.lockCommand(commandType, (byte) 0, database.getDbId(), lockKey,
                new byte[16], timeout, expried, count, rCount, null);
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
                nativeHandle = NativeBridge.readWriteLockCreate(nativeDatabaseHandle(), lockKey,
                        timeout, expried);
            }
            return nativeHandle;
        }
    }

    /**
     * Jaslock caches the write lock but builds a fresh read lock for every
     * {@code acquireRead}. Rust retains the cached write timing and applies the
     * forwarded packed timing to read acquires, so the shared handle timing is
     * updated here.
     */
    @Override
    void nativeTimingChanged() {
        long handle;
        synchronized (this) {
            handle = nativeHandle;
        }
        applyNativeTiming(handle);
    }
}
