package io.github.snower.jaslock;

import io.github.snower.jaslock.commands.ICommand;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Focused, Slock-free checks for the lazy native-handle contract of the counted
 * primitives.
 *
 * <p>Jaslock builds its cached lock/flow on the first operation, so timeout,
 * expiry, count and priority setters that run before that first operation must
 * be copied into the native handle. These tests pin the Java side of that
 * contract: construction and setters must not create the native handle, while
 * the compatibility fields must already reflect the setters.
 */
public class PrimitiveLazyInitTest {
    private static final class RecordingDatabase extends SlockDatabase {
        private int calls;

        RecordingDatabase() {
            super(null, (byte) 0, (short) 0, (short) 0);
        }

        @Override
        long nativeHandle() {
            calls++;
            throw new AssertionError("native handle created before the first operation");
        }
    }

    @Test
    public void facadesDeferNativeCreationAndAcceptPreOperationSetters() {
        RecordingDatabase database = new RecordingDatabase();
        byte[] key = "lazy".getBytes(StandardCharsets.UTF_8);

        Semaphore semaphore = new Semaphore(database, key, (short) 2, 0, 0);
        semaphore.setTimeout((short) 7);
        semaphore.setExpriedFlag((short) ICommand.EXPRIED_FLAG_MILLISECOND_TIME);
        semaphore.setCount((short) 3);
        assertEquals(7, semaphore.getTimeout());
        assertEquals(ICommand.EXPRIED_FLAG_MILLISECOND_TIME, semaphore.getExpriedFlag());
        assertEquals(3, semaphore.getCount());

        ReentrantLock reentrantLock = new ReentrantLock(database, key, 0, 0);
        reentrantLock.setTimeout((short) 8);
        reentrantLock.setExpried((short) 9);
        assertEquals(8, reentrantLock.getTimeout());
        assertEquals(9, reentrantLock.getExpried());

        ReadWriteLock readWriteLock = new ReadWriteLock(database, key, 0, 0);
        readWriteLock.setTimeoutFlag((short) ICommand.TIMEOUT_FLAG_MILLISECOND_TIME);
        readWriteLock.setExpriedFlag((short) ICommand.EXPRIED_FLAG_MILLISECOND_TIME);
        assertEquals(ICommand.TIMEOUT_FLAG_MILLISECOND_TIME, readWriteLock.getTimeoutFlag());
        assertEquals(ICommand.EXPRIED_FLAG_MILLISECOND_TIME, readWriteLock.getExpriedFlag());

        PriorityLock priorityLock = new PriorityLock(database, key, (byte) 1, 0, 0, (short) 2);
        priorityLock.setPriority((byte) 9);
        priorityLock.setCount((short) 4);
        assertEquals(9, priorityLock.getPriority());
        assertEquals(4, priorityLock.getCount());

        MaxConcurrentFlow maxConcurrentFlow =
                new MaxConcurrentFlow(database, key, (short) 5, 0, 60);
        maxConcurrentFlow.setExpriedFlag((short) ICommand.EXPRIED_FLAG_MILLISECOND_TIME);
        maxConcurrentFlow.setCount((short) 2);
        maxConcurrentFlow.setPriority((byte) 5);
        assertEquals(ICommand.EXPRIED_FLAG_MILLISECOND_TIME,
                maxConcurrentFlow.getExpriedFlag());
        assertEquals(2, maxConcurrentFlow.getCount());
        assertEquals(5, maxConcurrentFlow.getPriority());

        TokenBucketFlow tokenBucketFlow =
                new TokenBucketFlow(database, key, (short) 5, 0, 0.1);
        tokenBucketFlow.setTimeout((short) 11);
        tokenBucketFlow.setCount((short) 1);
        tokenBucketFlow.setPriority((byte) 3);
        assertEquals(11, tokenBucketFlow.getTimeout());
        assertEquals(1, tokenBucketFlow.getCount());
        assertEquals(3, tokenBucketFlow.getPriority());

        assertEquals("no native handle may be created before an operation", 0, database.calls);
    }
}
