package io.github.snower.jaslock;

import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.exceptions.SlockException;

import java.nio.charset.StandardCharsets;

/**
 * Jaslock-compatible tree lock facade backed by the Rust
 * {@code BindingTreeLock}/{@code BindingTreeLeafLock} primitives.
 *
 * <p>The hierarchy relation, the child/parent check-lock sequence and the leaf
 * lifecycle are executed by Rust. Java only keeps the caller-visible key,
 * parent and root flag for compatibility and forwards operations to typed native
 * handles. There is no Java fallback that rebuilds the legacy check-lock
 * sequence or loops/retries parent checks.
 *
 * <p>The native contract used here is:
 * <ul>
 *   <li>{@link NativeBridge#treeLockCreate} creates a root/parented handle;
 *       a {@code null} parent marks a root.</li>
 *   <li>{@link NativeBridge#treeLeafCreate} creates an explicit leaf for an
 *       already normalized 16-byte id.</li>
 *   <li>{@link NativeBridge#OP_TREE_ACQUIRE},
 *       {@link NativeBridge#OP_TREE_RELEASE} and
 *       {@link NativeBridge#OP_TREE_WAIT} map to
 *       {@code BindingTreeLock::{acquire,release,wait_packed}}.</li>
 *   <li>{@link NativeBridge#OP_TREE_LEAF_ACQUIRE} and
 *       {@link NativeBridge#OP_TREE_LEAF_RELEASE} map to
 *       {@code BindingTreeLeafLock::{acquire,release}}.</li>
 * </ul>
 */
public class TreeLock extends AbstractExecution {
    private final byte[] parentKey;
    private final boolean isRoot;
    private final long nativeHandle;

    public TreeLock(SlockDatabase database, byte[] parentKey, byte[] lockKey, int timeout, int expried) {
        super(database, lockKey, timeout, expried);

        // Keep the caller-visible parent exactly as supplied (legacy
        // getParentKey()), but the native factory requires a fixed-width
        // 16-byte parent, so normalize only the value handed to Rust.
        this.parentKey = parentKey;
        this.isRoot = parentKey == null;
        byte[] nativeParentKey = parentKey == null
                ? null : PrimitiveSupport.normalizeLockId(parentKey);
        this.nativeHandle = NativeBridge.treeLockCreate(database.nativeHandle(), nativeParentKey,
                this.lockKey, timeout, expried);
        // Seed the authoritative packed timing after Rust merged the database
        // defaults, so later setters replace the exact same bit pattern.
        applyNativeMetadata(NativeBridge.metadata(nativeHandle));
    }

    public TreeLock(SlockDatabase database, String parentKey, String lockKey, int timeout, int expried) {
        this(database, parentKey.getBytes(StandardCharsets.UTF_8),
                lockKey.getBytes(StandardCharsets.UTF_8), timeout, expried);
    }

    public TreeLock(SlockDatabase database, byte[] lockKey, int timeout, int expried) {
        this(database, null, lockKey, timeout, expried);
    }

    public TreeLock(SlockDatabase database, String lockKey, int timeout, int expried) {
        this(database, null, lockKey.getBytes(StandardCharsets.UTF_8), timeout, expried);
    }

    public TreeLeafLock newLeafLock() {
        return new TreeLeafLock(database, this, LockCommand.genLockId());
    }

    public TreeLeafLock loadLeafLock(byte[] lockId) {
        return new TreeLeafLock(database, this, lockId);
    }

    public TreeLock newChild() {
        return new TreeLock(database, lockKey, LockCommand.genLockId(), timeout, expried);
    }

    public TreeLock loadChild(byte[] lockKey) {
        return new TreeLock(database, this.lockKey, lockKey, timeout, expried);
    }

    public void acquire() throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_TREE_ACQUIRE, 0, null);
        PrimitiveSupport.throwIfError(result, command);
    }

    public void release() throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK,
                ICommand.UNLOCK_FLAG_UNLOCK_TREE_LOCK);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_TREE_RELEASE, 0, null);
        PrimitiveSupport.throwIfError(result, command);
    }

    public void wait(int timeout) throws SlockException {
        LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0);
        NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                NativeBridge.OP_TREE_WAIT, timeout, null);
        PrimitiveSupport.throwIfError(result, command);
    }

    public byte[] getParentKey() {
        return parentKey;
    }

    public byte[] getLockKey() {
        return lockKey;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public AutoCloseable with() throws SlockException {
        acquire();
        return this::release;
    }

    /** Tree acquire/wait and new child/leaf creation read current timing. */
    @Override
    void nativeTimingChanged() {
        applyNativeTiming(nativeHandle);
    }

    private LockCommand command(byte commandType, byte flag) {
        return PrimitiveSupport.lockCommand(commandType, flag, database.getDbId(), lockKey,
                null, timeout, expried, (short) 0, (byte) 0, null);
    }

    public class TreeLeafLock {
        private final SlockDatabase database;
        private final byte[] lockKey;
        private final byte[] lockId;
        private final long nativeHandle;
        // Legacy TreeLeafLock owns a separate Lock snapshot; later TreeLock
        // timing setters must not rewrite this leaf's command metadata.
        private final int leafTimeout;
        private final int leafExpried;

        public TreeLeafLock(SlockDatabase database, TreeLock treeLock, Lock lock) {
            this(database, treeLock, lock == null ? null : lock.getLockId());
        }

        private TreeLeafLock(SlockDatabase database, TreeLock treeLock, byte[] lockId) {
            this.database = database;
            byte[] id = PrimitiveSupport.normalizeLockId(lockId);
            if (id == null) {
                id = LockCommand.genLockId();
            }
            this.lockId = id;
            this.lockKey = treeLock.getLockKey();
            this.leafTimeout = treeLock.timeout;
            this.leafExpried = treeLock.expried;
            this.nativeHandle = NativeBridge.treeLeafCreate(treeLock.nativeHandle, id);
        }

        public void acquire() throws SlockException {
            LockCommand command = command(ICommand.COMMAND_TYPE_LOCK, (byte) 0);
            NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                    NativeBridge.OP_TREE_LEAF_ACQUIRE, 0, null);
            PrimitiveSupport.throwIfError(result, command);
        }

        public void release() throws SlockException {
            LockCommand command = command(ICommand.COMMAND_TYPE_UNLOCK,
                    ICommand.UNLOCK_FLAG_UNLOCK_TREE_LOCK);
            NativeBridge.Result result = PrimitiveSupport.submitAndWait(database, nativeHandle,
                    NativeBridge.OP_TREE_LEAF_RELEASE, 0, null);
            PrimitiveSupport.throwIfError(result, command);
        }

        public byte[] getLockKey() {
            return lockKey;
        }

        public byte[] getLockId() {
            return lockId;
        }

        private LockCommand command(byte commandType, byte flag) {
            return PrimitiveSupport.lockCommand(commandType, flag, database.getDbId(), lockKey,
                    lockId, leafTimeout, leafExpried, (short) 0, (byte) 0, null);
        }
    }
}
