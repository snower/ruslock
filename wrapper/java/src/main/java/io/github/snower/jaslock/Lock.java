package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.commands.CommandResult;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.commands.LockCommandResult;
import io.github.snower.jaslock.datas.LockData;
import io.github.snower.jaslock.exceptions.LockLockedException;
import io.github.snower.jaslock.exceptions.LockNotOwnException;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class Lock extends AbstractExecution {
    private byte[] lockId;
    private final long nativeHandle;

    public byte[] getLockKey() {
        return lockKey;
    }

    public byte[] getLockId() {
        return lockId;
    }

    public Lock(SlockDatabase database, byte[] lockKey, byte[] lockId, int timeout, int expried, short count, byte rCount) {
        super(database, lockKey, timeout, expried, count, rCount);

        this.lockId = PrimitiveSupport.normalizeLockId(lockId);
        this.nativeHandle = PrimitiveSupport.createLock(database, this.lockKey, this.lockId,
                timeout, expried, count, rCount);
        NativeBridge.Metadata metadata = NativeBridge.metadata(nativeHandle);
        applyNativeMetadata(metadata);
        if (metadata.lockId != null) {
            this.lockId = metadata.lockId;
        }
    }

    public Lock(SlockDatabase database, byte[] lockKey, int timeout, int expried) {
        this(database, lockKey, null, timeout, expried, (short) 0, (byte) 0);
    }

    public Lock(SlockDatabase database, String lockKey, int timeout, int expried) {
        this(database, lockKey.getBytes(StandardCharsets.UTF_8), null, timeout, expried, (short) 0, (byte) 0);
    }

    public LockCommandResult acquire(byte flag) throws SlockException {
        return acquire(flag, (LockData) null);
    }

    public LockCommandResult acquire(byte flag, LockData lockData) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, flag, lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_LOCK_ACQUIRE, flag & 0xff, lockData);
        refreshCurrentLockData(nativeHandle);
        PrimitiveSupport.throwIfError(result, command);
        return (LockCommandResult) PrimitiveSupport.commandResult(result);
    }

    public void acquire(byte flag, Consumer<CallbackCommandResult> callback) throws SlockException {
        acquire(flag, null, callback);
    }

    public void acquire(byte flag, LockData lockData, Consumer<CallbackCommandResult> callback) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, flag, lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_LOCK_ACQUIRE,
                flag & 0xff, lockData, result -> {
                    refreshCurrentLockData(nativeHandle);
                    callback.accept(PrimitiveSupport.callbackResult(command, result));
                });
    }

    public void acquire() throws SlockException {
        acquire((byte) 0, (LockData) null);
    }

    public void acquire(LockData lockData) throws SlockException {
        acquire((byte) 0, lockData);
    }

    public CallbackFuture<Boolean> acquire(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return acquire(null, callback);
    }

    public CallbackFuture<Boolean> acquire(LockData lockData, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        acquire((byte) 0, lockData, callbackCommandResult -> {
            try {
                callbackCommandResult.getResult();
                callbackFuture.setResult(true);
            } catch (SlockException e) {
                callbackFuture.setResult(false, e);
            }
        });
        return callbackFuture;
    }

    public LockCommandResult release(byte flag) throws SlockException {
        return release(flag, (LockData) null);
    }

    public LockCommandResult release(byte flag, LockData lockData) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK, flag, lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_LOCK_RELEASE, flag & 0xff, lockData);
        refreshCurrentLockData(nativeHandle);
        PrimitiveSupport.throwIfError(result, command);
        return (LockCommandResult) PrimitiveSupport.commandResult(result);
    }

    public void release(byte flag, Consumer<CallbackCommandResult> callback) throws SlockException {
        release(flag, null, callback);
    }

    public void release(byte flag, LockData lockData, Consumer<CallbackCommandResult> callback) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK, flag, lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_LOCK_RELEASE,
                flag & 0xff, lockData, result -> {
                    refreshCurrentLockData(nativeHandle);
                    callback.accept(PrimitiveSupport.callbackResult(command, result));
                });
    }

    public void release() throws SlockException {
        release((byte) 0, (LockData) null);
    }

    public void release(LockData lockData) throws SlockException {
        release((byte) 0, lockData);
    }

    public CallbackFuture<Boolean> release(Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        return release(null, callback);
    }

    public CallbackFuture<Boolean> release(LockData lockData, Consumer<CallbackFuture<Boolean>> callback) throws SlockException {
        CallbackFuture<Boolean> callbackFuture = new CallbackFuture<>(callback);
        release((byte) 0, lockData, callbackCommandResult -> {
            try {
                callbackCommandResult.getResult();
                callbackFuture.setResult(true);
            } catch (SlockException e) {
                callbackFuture.setResult(false, e);
            }
        });
        return callbackFuture;
    }

    public CommandResult show() throws SlockException {
        return show((LockData) null);
    }

    public CommandResult show(LockData lockData) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, ICommand.LOCK_FLAG_SHOW_WHEN_LOCKED, lockData);
        NativeBridge.Result result;
        if (lockData == null) {
            result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                    NativeBridge.OP_LOCK_SHOW, 0, null);
            refreshCurrentLockData(nativeHandle);
            PrimitiveSupport.throwIfError(result, command);
            if (result.lock != null
                    && result.lock.result == ICommand.COMMAND_RESULT_UNOWN_ERROR) {
                return PrimitiveSupport.toLockCommandResult(result.lock);
            }
            return null;
        }

        // The native show entry has no data parameter, so a data-carrying show
        // uses the equivalent acquire-with-show-flag primitive.
        result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_LOCK_ACQUIRE, ICommand.LOCK_FLAG_SHOW_WHEN_LOCKED, lockData);
        refreshCurrentLockData(nativeHandle);
        if (result.isOk()) {
            return null;
        }
        if (result.errorKind == NativeBridge.ERROR_LOCK_NOT_OWN && result.lock != null) {
            return PrimitiveSupport.toLockCommandResult(result.lock);
        }
        throw PrimitiveSupport.toException(result, command);
    }

    public void show(Consumer<CallbackCommandResult> callback) throws SlockException {
        show(null, callback);
    }

    public void show(LockData lockData, Consumer<CallbackCommandResult> callback) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, ICommand.LOCK_FLAG_SHOW_WHEN_LOCKED, lockData);
        if (lockData == null) {
            PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_LOCK_SHOW, 0,
                    null, result -> {
                        refreshCurrentLockData(nativeHandle);
                        if (!result.isOk()) {
                            callback.accept(PrimitiveSupport.callbackResult(command, result));
                            return;
                        }
                        CommandResult commandResult = null;
                        if (result.lock != null
                                && result.lock.result == ICommand.COMMAND_RESULT_UNOWN_ERROR) {
                            commandResult = PrimitiveSupport.toLockCommandResult(result.lock);
                        }
                        callback.accept(new CallbackCommandResult(command, commandResult, null));
                    });
            return;
        }

        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_LOCK_ACQUIRE,
                ICommand.LOCK_FLAG_SHOW_WHEN_LOCKED, lockData, result -> {
                    refreshCurrentLockData(nativeHandle);
                    if (result.isOk()) {
                        callback.accept(new CallbackCommandResult(command, null, null));
                        return;
                    }
                    if (result.errorKind == NativeBridge.ERROR_LOCK_NOT_OWN && result.lock != null) {
                        callback.accept(new CallbackCommandResult(command,
                                PrimitiveSupport.toLockCommandResult(result.lock), null));
                        return;
                    }
                    callback.accept(PrimitiveSupport.callbackResult(command, result));
                });
    }

    public void update() throws SlockException {
        update((LockData) null);
    }

    public void update(LockData lockData) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, ICommand.LOCK_FLAG_UPDATE_WHEN_LOCKED, lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_LOCK_UPDATE, 0, lockData);
        refreshCurrentLockData(nativeHandle);
        PrimitiveSupport.throwIfError(result, command);
    }

    public void update(Consumer<CallbackCommandResult> callback) throws SlockException {
        update(null, callback);
    }

    public void update(LockData lockData, Consumer<CallbackCommandResult> callback) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, ICommand.LOCK_FLAG_UPDATE_WHEN_LOCKED, lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_LOCK_UPDATE, 0,
                lockData, result -> {
                    refreshCurrentLockData(nativeHandle);
                    callback.accept(PrimitiveSupport.callbackResult(command, result));
                });
    }

    public void releaseHead() throws SlockException {
        releaseHead((LockData) null);
    }

    public void releaseHead(LockData lockData) throws SlockException {
        LockCommand command = releaseHeadCommand(lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_LOCK_RELEASE_HEAD, 0, lockData);
        refreshCurrentLockData(nativeHandle);
        PrimitiveSupport.throwIfError(result, command);
    }

    public void releaseHead(Consumer<CallbackCommandResult> callback) throws SlockException {
        releaseHead(null, callback);
    }

    public void releaseHead(LockData lockData, Consumer<CallbackCommandResult> callback) throws SlockException {
        LockCommand command = releaseHeadCommand(lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle, NativeBridge.OP_LOCK_RELEASE_HEAD,
                0, lockData, result -> {
                    refreshCurrentLockData(nativeHandle);
                    callback.accept(PrimitiveSupport.callbackResult(command, result));
                });
    }

    public LockCommandResult releaseHeadRetoLockWait() throws SlockException {
        return releaseHeadRetoLockWait((LockData) null);
    }

    public LockCommandResult releaseHeadRetoLockWait(LockData lockData) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK,
                (byte) (ICommand.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
                        | ICommand.UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT), lockData);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_LOCK_RELEASE_HEAD_TO_LOCK_WAIT, 0, lockData);
        refreshCurrentLockData(nativeHandle);
        PrimitiveSupport.throwIfError(result, command);
        return (LockCommandResult) PrimitiveSupport.commandResult(result);
    }

    public void releaseHeadRetoLockWait(Consumer<CallbackCommandResult> callback) throws SlockException {
        releaseHeadRetoLockWait(null, callback);
    }

    public void releaseHeadRetoLockWait(LockData lockData, Consumer<CallbackCommandResult> callback) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK,
                (byte) (ICommand.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
                        | ICommand.UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT), lockData);
        PrimitiveSupport.submitAsync(database, nativeHandle,
                NativeBridge.OP_LOCK_RELEASE_HEAD_TO_LOCK_WAIT, 0, lockData, result -> {
                    refreshCurrentLockData(nativeHandle);
                    callback.accept(PrimitiveSupport.callbackResult(command, result));
                });
    }

    public AutoCloseable with() throws SlockException {
        acquire();
        return this::release;
    }

    private LockCommand command(byte commandType, byte flag, LockData lockData) {
        return PrimitiveSupport.lockCommand(commandType, flag, database.getDbId(), lockKey,
                lockId, timeout, expried, count, rCount, lockData);
    }

    private LockCommand releaseHeadCommand(LockData lockData) {
        return PrimitiveSupport.lockCommand(ICommand.COMMAND_TYPE_UNLOCK,
                ICommand.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED, database.getDbId(),
                lockKey, new byte[16], timeout, expried, count, (byte) 0, lockData);
    }

    @Override
    void nativeTimingChanged() {
        applyNativeTiming(nativeHandle);
    }

    @Override
    void nativeConfigurationChanged() {
        if (nativeHandle == 0) {
            return;
        }
        try {
            PrimitiveSupport.updateLockCount(nativeHandle, NativeBridge.OP_LOCK_SET_COUNT,
                    count & 0xffff);
            PrimitiveSupport.updateLockCount(nativeHandle, NativeBridge.OP_LOCK_SET_R_COUNT,
                    rCount & 0xff);
        } catch (RuntimeException ignored) {
            // Setters retain their historical non-throwing signature.
        }
    }
}
