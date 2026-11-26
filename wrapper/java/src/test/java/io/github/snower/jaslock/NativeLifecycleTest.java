package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackFuture;
import io.github.snower.jaslock.callback.ExecutorOption;
import io.github.snower.jaslock.exceptions.EventWaitTimeoutException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JDK 8 lifecycle integration against a real Jaslock service.
 *
 * <p>The client is configured with a single callback worker so a native wait
 * running on the callback pool is immediately visible as callback starvation.
 */
public class NativeLifecycleTest {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 5658;

    private static SlockClient openSingleWorkerClient() throws IOException {
        SlockClient client = new SlockClient(HOST, PORT);
        client.enableAsyncCallback(new ExecutorOption(1, 1, 65536, 120, TimeUnit.SECONDS));
        client.open();
        return client;
    }

    @Test
    public void sameEventWaitAndSetStayBounded() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            Event event = client.newEvent("native_lifecycle_wait_set_" + System.nanoTime(),
                    5, 60, false);
            CountDownLatch waitDone = new CountDownLatch(1);
            AtomicInteger waitCalls = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
            CallbackFuture<Boolean> waitFuture = event.wait(30, callback -> {
                waitCalls.incrementAndGet();
                try {
                    callback.getResult();
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
                waitDone.countDown();
            });

            Thread.sleep(100L);
            event.set("lifecycle");

            Assert.assertTrue("same-object wait did not complete", waitDone.await(3, TimeUnit.SECONDS));
            Assert.assertEquals(1, waitCalls.get());
            Assert.assertNull(failure.get());
            Assert.assertTrue(waitFuture.isDone());
            Assert.assertTrue(waitFuture.getResult());
            Assert.assertEquals("lifecycle", event.getCurrentLockDataAsString());
        } finally {
            client.close();
        }
    }

    @Test
    public void pendingAcquireCallbacksDoNotStarveReleaseAndReentry() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            String key = "native_lifecycle_pending_" + System.nanoTime();
            Lock owner = client.newLock(key, 5, 60);
            owner.acquire();
            Event reentryEvent = client.newEvent(key + "_reentry", 5, 60, false);

            int pending = 16;
            List<Lock> contenders = new ArrayList<Lock>();
            CountDownLatch cycles = new CountDownLatch(pending);
            CountDownLatch firstRelease = new CountDownLatch(1);
            CountDownLatch reentry = new CountDownLatch(1);
            AtomicInteger failures = new AtomicInteger();

            for (int i = 0; i < pending; i++) {
                final Lock contender = client.newLock(key, 5, 60);
                contenders.add(contender);
                contender.acquire(acquire -> {
                    try {
                        acquire.getResult();
                        contender.release(release -> {
                            try {
                                release.getResult();
                            } catch (Throwable e) {
                                failures.incrementAndGet();
                            }
                            if (firstRelease.getCount() > 0) {
                                firstRelease.countDown();
                                try {
                                    reentryEvent.set(callback -> {
                                        try {
                                            callback.getResult();
                                        } catch (Throwable e) {
                                            failures.incrementAndGet();
                                        }
                                        reentry.countDown();
                                    });
                                } catch (Throwable e) {
                                    failures.incrementAndGet();
                                    reentry.countDown();
                                }
                            }
                            cycles.countDown();
                        });
                    } catch (Throwable e) {
                        failures.incrementAndGet();
                        cycles.countDown();
                    }
                });
            }

            owner.release();

            Assert.assertTrue("release callback was starved by pending acquires",
                    firstRelease.await(3, TimeUnit.SECONDS));
            Assert.assertTrue("callback reentry did not complete",
                    reentry.await(3, TimeUnit.SECONDS));
            Assert.assertTrue("pending acquire/release cycles did not complete",
                    cycles.await(15, TimeUnit.SECONDS));
            Assert.assertEquals("callback/reentry failures", 0, failures.get());
        } finally {
            client.close();
        }
    }

    @Test
    public void priorityLockCallbackCycleDoesNotStarveWithSingleWorker() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            String key = "native_lifecycle_priority_" + System.nanoTime();
            int pending = 12;
            CountDownLatch cycles = new CountDownLatch(pending);
            AtomicInteger failures = new AtomicInteger();

            for (int i = 0; i < pending; i++) {
                final PriorityLock lock = client.newPriorityLock(key, (byte) (i + 1), 5, 60);
                lock.acquire(acquire -> {
                    try {
                        acquire.getResult();
                        lock.release(release -> {
                            try {
                                release.getResult();
                            } catch (Throwable e) {
                                failures.incrementAndGet();
                            }
                            cycles.countDown();
                        });
                    } catch (Throwable e) {
                        failures.incrementAndGet();
                        cycles.countDown();
                    }
                });
            }

            Assert.assertTrue("priority acquire/release cycle was starved",
                    cycles.await(15, TimeUnit.SECONDS));
            Assert.assertEquals("priority callback failures", 0, failures.get());
        } finally {
            client.close();
        }
    }

    @Test
    public void deliveredCallbackIsNotRepeatedByClose() throws Exception {
        SlockClient client = openSingleWorkerClient();
        boolean closed = false;
        try {
            Event event = client.newEvent("native_lifecycle_close_" + System.nanoTime(),
                    5, 60, false);
            CountDownLatch delivered = new CountDownLatch(1);
            AtomicInteger calls = new AtomicInteger();
            event.set(callback -> {
                calls.incrementAndGet();
                delivered.countDown();
            });

            Assert.assertTrue("callback was not delivered", delivered.await(3, TimeUnit.SECONDS));
            client.close();
            closed = true;
            Thread.sleep(100L);
            Assert.assertEquals("close must not repeat a delivered callback", 1, calls.get());
        } finally {
            if (!closed) {
                client.close();
            }
        }
    }

    @Test
    public void closeAndPendingWaitRaceNotifiesAtMostOnce() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            Event event = client.newEvent("native_lifecycle_close_race_" + System.nanoTime(),
                    5, 60, false);
            AtomicInteger calls = new AtomicInteger();
            event.wait(30, callback -> calls.incrementAndGet());

            Thread.sleep(100L);
            client.close();
            Thread.sleep(200L);

            Assert.assertTrue("close race notified a callback more than once", calls.get() <= 1);
        } finally {
            client.close();
        }
    }

    @Test
    public void timingSettersUpdateLiveNativeHandlesInPlace() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            String suffix = "_" + System.nanoTime();

            Lock lock = client.newLock("native_lifecycle_lock_timing" + suffix, 5, 60);
            long lockHandle = nativeHandleOf(lock);
            NativeBridge.Metadata lockBefore = NativeBridge.metadata(lockHandle);
            Assert.assertEquals(5L, lockBefore.timeoutBits);
            Assert.assertEquals(60L, lockBefore.expiredBits);

            lock.setTimeout((short) 7);
            lock.setExpried((short) 11);
            NativeBridge.Metadata lockLow = NativeBridge.metadata(lockHandle);
            Assert.assertEquals("lock setTimeout must reach the live native handle",
                    7L, lockLow.timeoutBits);
            Assert.assertEquals("lock setExpried must reach the live native handle",
                    11L, lockLow.expiredBits);
            Assert.assertArrayEquals("lock timing setters must not recreate the native object",
                    lockBefore.lockId, lockLow.lockId);

            lock.acquire();
            lock.release();

            lock.setTimeoutFlag((short) 2);
            lock.setExpriedFlag((short) 3);
            NativeBridge.Metadata lockHigh = NativeBridge.metadata(lockHandle);
            Assert.assertEquals("lock setTimeoutFlag must preserve low timeout bits",
                    ((2L << 16) | 7L), lockHigh.timeoutBits);
            Assert.assertEquals("lock setExpriedFlag must preserve low expiry bits",
                    ((3L << 16) | 11L), lockHigh.expiredBits);
            Assert.assertArrayEquals("post-operation timing setter must keep lock identity",
                    lockBefore.lockId, lockHigh.lockId);

            Event event = client.newEvent("native_lifecycle_event_timing" + suffix, 5, 60, true);
            long eventHandle = nativeHandleOf(event);
            NativeBridge.Metadata eventBefore = NativeBridge.metadata(eventHandle);
            Assert.assertEquals(5L, eventBefore.timeoutBits);
            Assert.assertEquals(60L, eventBefore.expiredBits);

            event.setTimeout((short) 9);
            event.setExpried((short) 13);
            NativeBridge.Metadata eventAfter = NativeBridge.metadata(eventHandle);
            Assert.assertEquals("event setTimeout must reach the live native handle",
                    9L, eventAfter.timeoutBits);
            Assert.assertEquals("event setExpried must reach the live native handle",
                    13L, eventAfter.expiredBits);
            Assert.assertArrayEquals("event timing setters must not recreate the native object",
                    eventBefore.key, eventAfter.key);
            Assert.assertTrue("event must remain usable after a timing change", event.isSet());
        } finally {
            client.close();
        }
    }

    @Test
    public void clearWithDataPublishesPayloadForDefaultSetEvent() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            Event event = client.newEvent("native_lifecycle_clear_set_" + UUID.randomUUID(),
                    5, 60, true);
            Assert.assertTrue(event.isSet());

            event.clear("default-set-clear-data");
            Assert.assertFalse("default-set clear(data) must clear the event", event.isSet());

            event.set();
            Assert.assertTrue(event.isSet());
            event.wait(5);
            Assert.assertEquals("default-set clear(data) must publish its payload",
                    "default-set-clear-data", event.getCurrentLockDataAsString());
        } finally {
            client.close();
        }
    }

    @Test
    public void clearWithDataPublishesPayloadForDefaultUnsetEvent() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            Event event = client.newEvent("native_lifecycle_clear_unset_" + UUID.randomUUID(),
                    5, 60, false);
            Assert.assertFalse(event.isSet());
            event.set("seed");
            Assert.assertTrue(event.isSet());

            event.clear("default-unset-clear-data");
            Assert.assertFalse("default-unset clear(data) must clear the event", event.isSet());

            event.set();
            Assert.assertTrue(event.isSet());
            event.wait(5);
            Assert.assertEquals("default-unset clear(data) must publish its payload",
                    "default-unset-clear-data", event.getCurrentLockDataAsString());
        } finally {
            client.close();
        }
    }

    @Test
    public void waitAndTimeoutRetryClearSucceedsForBothPolarities() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            Event defaultSet = client.newEvent(
                    "native_lifecycle_retry_success_set_" + UUID.randomUUID(), 5, 60, true);
            Assert.assertTrue(defaultSet.isSet());
            defaultSet.waitAndTimeoutRetryClear(2);
            Assert.assertTrue("default-set success path leaves the event set",
                    defaultSet.isSet());

            Event defaultUnset = client.newEvent(
                    "native_lifecycle_retry_success_unset_" + UUID.randomUUID(), 5, 60, false);
            Assert.assertFalse(defaultUnset.isSet());
            defaultUnset.set();
            Assert.assertTrue(defaultUnset.isSet());
            defaultUnset.waitAndTimeoutRetryClear(2);
            Assert.assertFalse("default-unset success path must clear the event",
                    defaultUnset.isSet());
        } finally {
            client.close();
        }
    }

    @Test
    public void waitAndTimeoutRetryClearTimeoutHonorsPolarityRules() throws Exception {
        SlockClient client = openSingleWorkerClient();
        try {
            Event defaultSet = client.newEvent(
                    "native_lifecycle_retry_timeout_set_" + UUID.randomUUID(), 5, 60, true);
            defaultSet.clear();
            Assert.assertFalse(defaultSet.isSet());
            try {
                defaultSet.waitAndTimeoutRetryClear(1);
                Assert.fail("default-set wait timeout must throw EventWaitTimeoutException");
            } catch (EventWaitTimeoutException expected) {
                // Baseline Jaslock converts the wait timeout for both polarities.
            }
            Assert.assertFalse("default-set timeout must leave the event cleared",
                    defaultSet.isSet());

            Event defaultUnset = client.newEvent(
                    "native_lifecycle_retry_timeout_unset_" + UUID.randomUUID(), 5, 60, false);
            Assert.assertFalse(defaultUnset.isSet());
            try {
                defaultUnset.waitAndTimeoutRetryClear(1);
                Assert.fail("default-unset wait timeout must throw EventWaitTimeoutException");
            } catch (EventWaitTimeoutException expected) {
                // Stable Jaslock converts the wait timeout directly for this polarity.
            }
        } finally {
            client.close();
        }
    }

    private static long nativeHandleOf(Object primitive) throws Exception {
        Field field = primitive.getClass().getDeclaredField("nativeHandle");
        field.setAccessible(true);
        return field.getLong(primitive);
    }
}
