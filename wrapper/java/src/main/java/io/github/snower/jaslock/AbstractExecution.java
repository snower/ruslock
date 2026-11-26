package io.github.snower.jaslock;

import io.github.snower.jaslock.datas.LockResultData;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Common configuration, metadata and result-data surface for RusLock primitives.
 *
 * <p>The public and protected API is kept compatible with Jaslock. Concrete
 * primitives own an opaque {@code long} handle created by {@link NativeBridge};
 * this class holds only the caller-visible configuration and the last lock data
 * returned by the native primitive. No lock algorithm is executed in Java.
 *
 * <p>Package-private hooks for follow-up primitive implementations:
 * <ul>
 *   <li>{@link #nativeDatabaseHandle()} obtains the {@link SlockDatabase}'s opaque
 *       handle for native object construction.</li>
 *   <li>{@link #applyNativeMetadata(NativeBridge.Metadata)} seeds the protected
 *       configuration fields and current lock data from the native object right
 *       after construction.</li>
 *   <li>{@link #refreshCurrentLockData(long)} mirrors only the native current
 *       lock data after an operation. It deliberately does not overwrite timeout,
 *       expiry or count fields so local setters keep their compatibility
 *       semantics.</li>
 *   <li>{@link #nativeConfigurationChanged()} is invoked by {@link #setCount(short)}
 *       and {@link #setRCount(byte)}. A primitive that can push a count change to
 *       Rust overrides it; the base implementation is intentionally empty.</li>
 *   <li>{@link #nativeTimingChanged()} is invoked by every timeout/expiry setter
 *       after the exact packed-int mutation. A primitive forwards both complete
 *       packed fields to its native handle when the binding exposes timing
 *       mutation; the base implementation is intentionally empty.</li>
 * </ul>
 *
 * <p>The protected timing fields remain the authoritative Java compatibility
 * state. Native mutation, when available, receives the full packed values rather
 * than reinterpreting individual halves, so database defaults already merged at
 * construction are never applied twice.
 */
public abstract class AbstractExecution {
    protected final SlockDatabase database;
    protected byte[] lockKey;
    protected int timeout;
    protected int expried;
    protected short count;
    protected byte rCount;
    protected LockResultData currentLockData;

    public AbstractExecution(SlockDatabase database, byte[] lockKey, int timeout, int expried, short count, byte rCount) {
        this.database = database;
        if(lockKey.length > 16) {
            try {
                MessageDigest digest = MessageDigest.getInstance("MD5");
                this.lockKey = digest.digest(lockKey);
            } catch (NoSuchAlgorithmException e) {
                this.lockKey = Arrays.copyOfRange(lockKey, 0, 16);
            }
        } else {
            this.lockKey = new byte[16];
            System.arraycopy(lockKey, 0, this.lockKey, 16 - lockKey.length, lockKey.length);
        }
        this.timeout = timeout;
        this.expried = expried;
        this.count = count;
        this.rCount = rCount;
    }

    public AbstractExecution(SlockDatabase database, byte[] lockKey, int timeout, int expried) {
        this(database, lockKey, timeout, expried, (short)0, (byte)0);
    }

    public void setTimeout(short timeout) {
        this.timeout = (((int) timeout) & 0xffff) | (this.timeout & 0xffff0000);
        nativeTimingChanged();
    }

    public short getTimeout() {
        return (short) timeout;
    }

    public void setTimeoutFlag(short timeoutFlag) {
        this.timeout = ((((int) timeoutFlag) & 0xffff) << 16) | (this.timeout & 0xffff);
        nativeTimingChanged();
    }

    public void updateTimeoutFlag(short timeoutFlag) {
        this.timeout = ((((int) timeoutFlag) & 0xffff) << 16) | this.timeout;
        nativeTimingChanged();
    }

    public short getTimeoutFlag() {
        return (short) (timeout >> 16);
    }

    public void setExpried(short expried) {
        this.expried = (((int) expried) & 0xffff) | (this.expried & 0xffff0000);
        nativeTimingChanged();
    }

    public short getExpried() {
        return (short) expried;
    }

    public void setExpriedFlag(short expriedFlag) {
        this.expried = ((((int) expriedFlag) & 0xffff) << 16) | (this.expried & 0xffff);
        nativeTimingChanged();
    }

    public void updateExpriedFlag(short expriedFlag) {
        this.expried = ((((int) expriedFlag) & 0xffff) << 16) | this.expried;
        nativeTimingChanged();
    }

    public short getExpriedFlag() {
        return (short) (expried >> 16);
    }

    public void setCount(short count) {
        this.count = count;
        nativeConfigurationChanged();
    }

    public short getCount() {
        return count;
    }

    public void setRCount(byte rCount) {
        this.rCount = rCount;
        nativeConfigurationChanged();
    }

    public byte getRCount() {
        return rCount;
    }

    public LockResultData getCurrentLockData() {
        return currentLockData;
    }

    public byte[] getCurrentLockDataAsBytes() {
        if (currentLockData == null) {
            return null;
        }
        return currentLockData.getDataAsBytes();
    }

    public String getCurrentLockDataAsString() {
        if (currentLockData == null) {
            return null;
        }
        return currentLockData.getDataAsString();
    }

    public Long getCurrentLockDataAsLong() {
        if (currentLockData == null) {
            return 0L;
        }
        return currentLockData.getDataAsLong();
    }

    public List<byte[]> getCurrentLockDataAsList() {
        if (currentLockData == null) {
            return null;
        }
        return currentLockData.getRawDataAsList();
    }

    public List<String> getCurrentLockDataAsStringList() {
        if (currentLockData == null) {
            return null;
        }
        return currentLockData.getRawDataAsStringList();
    }

    public Map<String, byte[]> getCurrentLockDataAsMap() {
        if (currentLockData == null) {
            return null;
        }
        return currentLockData.getRawDataAsMap();
    }

    public Map<String, String> getCurrentLockDataAsStringMap() {
        if (currentLockData == null) {
            return null;
        }
        return currentLockData.getRawDataAsStringMap();
    }

    /** Returns the opaque native database handle used to construct a primitive. */
    final long nativeDatabaseHandle() {
        return database.nativeHandle();
    }

    /**
     * Seeds protected configuration and current data from the native handle.
     *
     * <p>Called once by a concrete primitive after its native object exists. The
     * native values are authoritative for key normalization, generated lock ids
     * and merged default timeout/expiry flags.
     */
    final void applyNativeMetadata(NativeBridge.Metadata metadata) {
        if (metadata.key != null) {
            this.lockKey = metadata.key;
        }
        this.timeout = (int) metadata.timeoutBits;
        this.expried = (int) metadata.expiredBits;
        this.count = (short) metadata.count;
        this.rCount = (byte) metadata.rCount;
        this.currentLockData = metadata.currentData == null ? null : new LockResultData(metadata.currentData);
    }

    /** Mirrors the native current lock data without touching configuration fields. */
    final void refreshCurrentLockData(long nativeHandle) {
        NativeBridge.Metadata metadata = NativeBridge.metadata(nativeHandle);
        this.currentLockData = metadata.currentData == null ? null : new LockResultData(metadata.currentData);
    }

    /**
     * Pushes the current packed timing to an existing native handle.
     *
     * <p>The protected fields are already the exact Jaslock packed-int state
     * (database defaults were merged once at construction and applied through
     * {@link #applyNativeMetadata(NativeBridge.Metadata)}), so they are sent
     * verbatim and native must not re-merge. The handle is never recreated and
     * the historical non-throwing setter contract is preserved.
     */
    final void applyNativeTiming(long nativeHandle) {
        if (nativeHandle == 0L) {
            return;
        }
        try {
            NativeBridge.primitiveSetTiming(nativeHandle, timeout, expried);
        } catch (RuntimeException ignored) {
            // Compatibility setters must not throw; the Java fields stay
            // authoritative if the native handle cannot accept the change.
        }
    }

    /**
     * Package hook invoked after any timeout or expiry setter.
     *
     * <p>The setter has already applied the exact Jaslock packed-int mutation to
     * {@link #timeout} and {@link #expried}; the hook forwards those complete
     * bit patterns. It runs before the next operation is submitted, so a setter
     * followed by the first acquire/wait uses the new timing immediately.
     *
     * <p>The default implementation is a no-op. Primitives with a native mutable
     * timing entry override it; the native side must replace the packed pattern
     * without re-merging database defaults.
     */
    void nativeTimingChanged() {
    }

    /**
     * Package hook invoked after {@link #setCount(short)} or
     * {@link #setRCount(byte)}.
     *
     * <p>The default implementation is a no-op. Primitives whose native binding
     * exposes the mutable count entries (for example {@code Lock}) override it.
     */
    void nativeConfigurationChanged() {
    }
}
