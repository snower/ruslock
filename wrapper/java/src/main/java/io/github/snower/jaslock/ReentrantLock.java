package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Reentrant lock facade over the dedicated Rust reentrant handle.
 *
 * <p>The single reusable lock identity and reentrant count live in Rust; this
 * class only submits one typed acquire/release per method.
 *
 * <p>The native handle is built on the first operation, matching Jaslock's
 * cached lock initialization. Jaslock never replaces that inner lock, so
 * timeout/expiry setters applied before the first operation are copied into the
 * handle while later changes intentionally do not reach the native lock.
 */
public class ReentrantLock extends AbstractExecution {
    private long nativeHandle;

    public ReentrantLock(SlockDatabase database, byte[] lockKey, int timeout, int expried) {
        super(database, lockKey, timeout, expried);
    }

    public ReentrantLock(SlockDatabase database, String lockKey, int timeout, int expried) {
        this(database, lockKey.getBytes(StandardCharsets.UTF_8), timeout, expried);
    }

    public void acquire() throws SlockException {
        submit(NativeBridge.OP_REENTRANT_ACQUIRE, command(ICommand.COMMAND_TYPE_LOCK));
    }

    public CallbackFuture<Boolean> acquire(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_REENTRANT_ACQUIRE, command(ICommand.COMMAND_TYPE_LOCK), result -> {
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
        submit(NativeBridge.OP_REENTRANT_RELEASE, command(ICommand.COMMAND_TYPE_UNLOCK));
    }

    public CallbackFuture<Boolean> release(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_REENTRANT_RELEASE, command(ICommand.COMMAND_TYPE_UNLOCK), result -> {
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
                nativeHandle = NativeBridge.reentrantLockCreate(nativeDatabaseHandle(), lockKey,
                        timeout, expried);
            }
            return nativeHandle;
        }
    }
}
