package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Semaphore facade over the dedicated Rust semaphore handle.
 *
 * <p>Count interpretation, generated acquire identities and unlock-first
 * release all execute in Rust. This class only converts arguments, submits one
 * typed operation per method and maps completion results back to Jaslock types.
 *
 * <p>Jaslock builds a fresh inner lock for every semaphore acquire and release,
 * so every operation uses the timeout, expiry and count current at that moment.
 * The native handle is therefore created on the first operation and later
 * timeout/expiry/count setters are forwarded to it before the next operation.
 */
public class Semaphore extends AbstractExecution {
    private long nativeHandle;

    public Semaphore(SlockDatabase database, byte[] semaphoreKey, short count, int timeout, int expried) {
        super(database, semaphoreKey, timeout, expried,
                (short) (count > 0 ? count - 1 : 0), (byte) 0);
    }

    public Semaphore(SlockDatabase database, String semaphoreKey, short count, int timeout, int expried) {
        this(database, semaphoreKey.getBytes(StandardCharsets.UTF_8), count, timeout, expried);
    }

    public void acquire() throws SlockException {
        submit(NativeBridge.OP_SEMAPHORE_ACQUIRE, command(ICommand.COMMAND_TYPE_LOCK));
    }

    public CallbackFuture<Boolean> acquire(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_SEMAPHORE_ACQUIRE, command(ICommand.COMMAND_TYPE_LOCK), result -> {
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
        submit(NativeBridge.OP_SEMAPHORE_RELEASE, command(ICommand.COMMAND_TYPE_UNLOCK));
    }

    public CallbackFuture<Boolean> release(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        submitAsync(NativeBridge.OP_SEMAPHORE_RELEASE, command(ICommand.COMMAND_TYPE_UNLOCK), result -> {
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
                nativeHandle = NativeBridge.semaphoreCreate(nativeDatabaseHandle(), lockKey,
                        nativeCount(), timeout, expried);
            }
            return nativeHandle;
        }
    }

    /**
     * Jaslock creates a fresh inner lock per semaphore operation, so packed
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
