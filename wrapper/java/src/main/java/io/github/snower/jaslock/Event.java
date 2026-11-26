package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.datas.LockData;
import io.github.snower.jaslock.datas.LockSetData;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Jaslock-compatible event facade backed by the Rust {@code BindingEvent}
 * primitive.
 *
 * <p>Every operation, including data-carrying clears and
 * {@link #waitAndTimeoutRetryClear(int)}, is submitted through
 * {@link NativeBridge} against the native event handle. Java only converts
 * arguments, waits for the completion and schedules callbacks.
 */
public class Event extends AbstractExecution {
    private final long nativeHandle;
    private final boolean defaultSeted;

    public Event(SlockDatabase database, byte[] eventKey, int timeout, int expried, boolean defaultSeted) {
        super(database, eventKey, timeout, expried);

        this.nativeHandle = PrimitiveSupport.createEvent(database, this.lockKey, timeout, expried,
                defaultSeted);
        NativeBridge.Metadata metadata = NativeBridge.metadata(nativeHandle);
        applyNativeMetadata(metadata);
        this.defaultSeted = metadata.flag();
    }

    public Event(SlockDatabase database, byte[] eventKey, int timeout, int expried) {
        this(database, eventKey, timeout, expried, true);
    }

    public Event(SlockDatabase database, String eventKey, int timeout, int expried, boolean defaultSeted) {
        this(database, eventKey.getBytes(StandardCharsets.UTF_8), timeout, expried, defaultSeted);
    }

    public Event(SlockDatabase database, String eventKey, int timeout, int expried) {
        this(database, eventKey.getBytes(StandardCharsets.UTF_8), timeout, expried, true);
    }

    public void clear() throws SlockException {
        clear((LockData) null);
    }

    public void clear(byte[] data) throws SlockException {
        clear(data != null ? new LockSetData(data) : null);
    }

    public void clear(String data) throws SlockException {
        clear(data != null ? new LockSetData(data) : null);
    }

    public void clear(LockData lockData) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_EVENT_CLEAR, 0, lockData);
        PrimitiveSupport.throwIfError(result, command);
    }

    public CallbackFuture<Boolean> clear(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return clear((LockData) null, callback);
    }

    public CallbackFuture<Boolean> clear(byte[] data, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return clear(data != null ? new LockSetData(data) : null, callback);
    }

    public CallbackFuture<Boolean> clear(String data, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return clear(data != null ? new LockSetData(data) : null, callback);
    }

    public CallbackFuture<Boolean> clear(LockData lockData, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_EVENT_CLEAR, 0,
                lockData, result -> {
                    if (result.isOk()) {
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false, PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public void set() throws SlockException {
        set((LockData) null);
    }

    public void set(byte[] data) throws SlockException {
        set(data != null ? new LockSetData(data) : null);
    }

    public void set(String data) throws SlockException {
        set(data != null ? new LockSetData(data) : null);
    }

    public void set(LockData lockData) throws SlockException {
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_EVENT_SET, 0, lockData);
        PrimitiveSupport.throwIfError(result, setCommand(lockData));
    }

    public CallbackFuture<Boolean> set(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return set((LockData) null, callback);
    }

    public CallbackFuture<Boolean> set(byte[] data, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return set(data != null ? new LockSetData(data) : null, callback);
    }

    public CallbackFuture<Boolean> set(String data, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return set(data != null ? new LockSetData(data) : null, callback);
    }

    public CallbackFuture<Boolean> set(LockData lockData, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = setCommand(lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_EVENT_SET, 0,
                lockData, result -> {
                    if (result.isOk()) {
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false, PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public boolean isSet() throws SlockException {
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_EVENT_IS_SET, 0, null);
        PrimitiveSupport.throwIfError(result, command(ICommand.COMMAND_TYPE_LOCK,
                (byte) 0, null));
        return result.hasValue && result.value;
    }

    public CallbackFuture<Boolean> isSet(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_EVENT_IS_SET, 0,
                null, result -> {
                    if (result.isOk()) {
                        callbackFuture.setResult(result.hasValue && result.value);
                    } else {
                        callbackFuture.setResult(false, PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public void wait(int timeout) throws SlockException {
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_EVENT_WAIT, timeout, null);
        if (result.isOk()) {
            refreshCurrentLockData(nativeHandle);
        }
        PrimitiveSupport.throwIfError(result, command(ICommand.COMMAND_TYPE_LOCK,
                (byte) 0, null));
    }

    public CallbackFuture<Boolean> wait(int timeout, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_EVENT_WAIT,
                timeout, null, result -> {
                    if (result.isOk()) {
                        refreshCurrentLockData(nativeHandle);
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false, PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public void waitAndTimeoutRetryClear(int timeout) throws SlockException {
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_EVENT_WAIT_RETRY_CLEAR, timeout, null);
        if (result.isOk()) {
            refreshCurrentLockData(nativeHandle);
        }
        PrimitiveSupport.throwIfError(result, command(ICommand.COMMAND_TYPE_LOCK,
                (byte) 0, null));
    }

    public CallbackFuture<Boolean> waitAndTimeoutRetryClear(int timeout, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        PrimitiveSupport.submitAsync(database, nativeHandle,
                NativeBridge.OP_EVENT_WAIT_RETRY_CLEAR, timeout, null, result -> {
                    if (result.isOk()) {
                        refreshCurrentLockData(nativeHandle);
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false, PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public AutoCloseable with() {
        return () -> {
            if (defaultSeted) {
                clear();
            } else {
                set();
            }
        };
    }

    /**
     * Forwards the exact packed timeout/expiry mutation to the live native
     * event. The handle is never recreated and the Java fields stay
     * authoritative for the Jaslock-compatible getters.
     */
    @Override
    void nativeTimingChanged() {
        applyNativeTiming(nativeHandle);
    }

    private LockCommand setCommand(LockData lockData) {
        if (defaultSeted) {
            return command(ICommand.COMMAND_TYPE_UNLOCK, (byte) 0, lockData);
        }
        return command(ICommand.COMMAND_TYPE_LOCK, ICommand.LOCK_FLAG_UPDATE_WHEN_LOCKED,
                lockData);
    }

    private LockCommand command(byte commandType, byte flag, LockData lockData) {
        return PrimitiveSupport.lockCommand(commandType, flag, database.getDbId(), lockKey,
                lockKey, timeout, expried, (short) 0, (byte) 0, lockData);
    }
}
