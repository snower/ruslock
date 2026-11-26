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
 * Jaslock-compatible group event facade backed by the Rust
 * {@code BindingGroupEvent} primitive.
 *
 * <p>Every operation is submitted through {@link NativeBridge} against the
 * native group-event handle. Java only converts arguments, schedules the
 * callback and reads back authoritative native state. The group-event version
 * progression, wakeup/retry-clear rules, current data and timeout mapping are
 * all owned by Rust; this class never replays the legacy Java lock composition
 * and never parses a returned lock id to advance the version.
 *
 * <p>The native contract used here is:
 * <ul>
 *   <li>{@link NativeBridge#groupEventCreate} creates the typed handle.</li>
 *   <li>{@link NativeBridge#OP_GROUP_EVENT_IS_SET},
 *       {@link NativeBridge#OP_GROUP_EVENT_SET},
 *       {@link NativeBridge#OP_GROUP_EVENT_CLEAR},
 *       {@link NativeBridge#OP_GROUP_EVENT_WAKEUP},
 *       {@link NativeBridge#OP_GROUP_EVENT_WAKEUP_WITH_DATA},
 *       {@link NativeBridge#OP_GROUP_EVENT_WAIT} and
 *       {@link NativeBridge#OP_GROUP_EVENT_WAIT_RETRY_CLEAR}
 *       execute the operations; {@code data} is the encoded
 *       {@link LockData#dumpData()} frame.</li>
 *   <li>{@link NativeBridge#groupEventVersionId} returns the version that Rust
 *       advanced during {@code wakeup}/{@code wait}/{@code waitAndTimeoutRetryClear}.</li>
 *   <li>{@link NativeBridge#metadata} exposes the group event's current lock
 *       data after a wait.</li>
 * </ul>
 */
public class GroupEvent extends AbstractExecution {
    private final long clientId;
    private final long nativeHandle;
    private long versionId;

    public GroupEvent(SlockDatabase database, byte[] groupKey, long clientId, long versionId, int timeout, int expried) {
        super(database, groupKey, timeout, expried);

        this.clientId = clientId;
        this.nativeHandle = NativeBridge.groupEventCreate(database.nativeHandle(), this.lockKey,
                clientId, versionId, timeout, expried);
        // Seed the authoritative packed timing after Rust merged the database
        // defaults, so later setters replace the exact same bit pattern.
        applyNativeMetadata(NativeBridge.metadata(nativeHandle));
        this.versionId = NativeBridge.groupEventVersionId(nativeHandle);
    }

    public GroupEvent(SlockDatabase database, String groupKey, long clientId, long versionId, int timeout, int expried) {
        this(database, groupKey.getBytes(StandardCharsets.UTF_8), clientId, versionId, timeout, expried);
    }

    public long getClientId() {
        return clientId;
    }

    public long getVersionId() {
        return versionId;
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
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK,
                ICommand.LOCK_FLAG_UPDATE_WHEN_LOCKED, lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_GROUP_EVENT_CLEAR, 0, lockData);
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
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK,
                ICommand.LOCK_FLAG_UPDATE_WHEN_LOCKED, lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_GROUP_EVENT_CLEAR, 0,
                lockData, result -> {
                    if (result.isOk()) {
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false,
                                PrimitiveSupport.toException(result, command));
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
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK,
                ICommand.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED, lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_GROUP_EVENT_SET, 0, lockData);
        PrimitiveSupport.throwIfError(result, command);
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
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK,
                ICommand.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED, lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_GROUP_EVENT_SET, 0,
                lockData, result -> {
                    if (result.isOk()) {
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false,
                                PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public boolean isSet() throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_GROUP_EVENT_IS_SET, 0, null);
        PrimitiveSupport.throwIfError(result, command);
        return result.hasValue && result.value;
    }

    public CallbackFuture<Boolean> isSet(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_GROUP_EVENT_IS_SET, 0,
                null, result -> {
                    if (result.isOk()) {
                        callbackFuture.setResult(result.hasValue && result.value);
                    } else {
                        callbackFuture.setResult(false,
                                PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public void wakeup() throws SlockException {
        wakeup((LockData) null);
    }

    public void wakeup(byte[] data) throws SlockException {
        wakeup(data != null ? new LockSetData(data) : null);
    }

    public void wakeup(String data) throws SlockException {
        wakeup(data != null ? new LockSetData(data) : null);
    }

    public void wakeup(LockData lockData) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK,
                (byte) (ICommand.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
                        | ICommand.UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT), lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                wakeupOperation(lockData), 0, lockData);
        if (result.isOk()) {
            refreshVersion();
        }
        PrimitiveSupport.throwIfError(result, command);
    }

    public CallbackFuture<Boolean> wakeup(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return wakeup((LockData) null, callback);
    }

    public CallbackFuture<Boolean> wakeup(byte[] data, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return wakeup(data != null ? new LockSetData(data) : null, callback);
    }

    public CallbackFuture<Boolean> wakeup(String data, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return wakeup(data != null ? new LockSetData(data) : null, callback);
    }

    public CallbackFuture<Boolean> wakeup(LockData lockData, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK,
                (byte) (ICommand.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
                        | ICommand.UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT), lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, wakeupOperation(lockData), 0,
                lockData, result -> {
                    if (result.isOk()) {
                        refreshVersion();
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false,
                                PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public void wait(int timeout) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_GROUP_EVENT_WAIT, timeout, null);
        if (result.isOk()) {
            refreshVersion();
            refreshCurrentLockData(nativeHandle);
        }
        PrimitiveSupport.throwIfError(result, command);
    }

    public CallbackFuture<Boolean> wait(int timeout, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_GROUP_EVENT_WAIT,
                timeout, null, result -> {
                    if (result.isOk()) {
                        refreshVersion();
                        refreshCurrentLockData(nativeHandle);
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false,
                                PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public void waitAndTimeoutRetryClear(int timeout) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_GROUP_EVENT_WAIT_RETRY_CLEAR, timeout, null);
        if (result.isOk()) {
            refreshVersion();
            refreshCurrentLockData(nativeHandle);
        }
        PrimitiveSupport.throwIfError(result, command);
    }

    public CallbackFuture<Boolean> waitAndTimeoutRetryClear(int timeout, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0, null);
        PrimitiveSupport.submitAsync(database, nativeHandle,
                NativeBridge.OP_GROUP_EVENT_WAIT_RETRY_CLEAR, timeout, null,
                result -> {
                    if (result.isOk()) {
                        refreshVersion();
                        refreshCurrentLockData(nativeHandle);
                        callbackFuture.setResult(true);
                    } else {
                        callbackFuture.setResult(false,
                                PrimitiveSupport.toException(result, command));
                    }
                });
        return callbackFuture;
    }

    public AutoCloseable with() {
        return this::wakeup;
    }

    /** Reads the version Rust advanced during the last wakeup/wait operation. */
    private void refreshVersion() {
        this.versionId = NativeBridge.groupEventVersionId(nativeHandle);
    }

    private int wakeupOperation(LockData lockData) {
        return lockData == null
                ? NativeBridge.OP_GROUP_EVENT_WAKEUP
                : NativeBridge.OP_GROUP_EVENT_WAKEUP_WITH_DATA;
    }

    /** Every group-event operation reads the current packed timing. */
    @Override
    void nativeTimingChanged() {
        applyNativeTiming(nativeHandle);
    }

    private LockCommand command(byte commandType, byte flag, LockData lockData) {
        // A generated descriptor id is fine here: the native operation owns the
        // actual group-event lock id/version. This class never encodes or parses
        // the version itself.
        return PrimitiveSupport.lockCommand(commandType, flag, database.getDbId(), lockKey,
                null, timeout, expried, (short) 0, (byte) 0, lockData);
    }
}
