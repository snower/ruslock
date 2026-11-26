package io.github.snower.jaslock;

import io.github.snower.jaslock.callback.CallbackCommandResult;
import io.github.snower.jaslock.callback.CallbackExecutorManager;
import io.github.snower.jaslock.commands.ICommand;
import io.github.snower.jaslock.commands.LockCommand;
import io.github.snower.jaslock.commands.PingCommand;
import io.github.snower.jaslock.exceptions.ClientAsyncCallbackDisabledException;
import io.github.snower.jaslock.exceptions.ClientClosedException;
import io.github.snower.jaslock.exceptions.ClientCommandTimeoutException;
import io.github.snower.jaslock.exceptions.ClientOutputStreamException;
import io.github.snower.jaslock.exceptions.ClientUnconnectException;
import io.github.snower.jaslock.exceptions.SlockException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Facade-only tests. No native library and no Jaslock server are required: the
 * single native client seam is replaced with an in-memory fake, so these tests
 * cover the Java compatibility surface, lazy handle creation, frame forwarding
 * and callback completion rules.
 */
public class FacadeClientTest {

    @Test
    public void constructionAndDatabaseSelectionAreLazy() throws Exception {
        FakeBackend backend = new FakeBackend();
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);

        SlockDatabase database = client.selectDatabase((byte) 3);
        Assert.assertEquals(3, database.getDbId());
        Assert.assertEquals(0, backend.runtimeCreates.get());
        Assert.assertEquals(0, backend.clientCreates.get());

        client.close();
        Assert.assertEquals(0, backend.clientCloseCalls.get());
        Assert.assertTrue(backend.releases.isEmpty());
    }

    @Test
    public void syncCommandUsesOneNativeClientAndRawFrame() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.response = pingResponse();
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        try {
            Assert.assertTrue(client.ping());

            Assert.assertEquals(1, backend.runtimeCreates.get());
            Assert.assertEquals(1, backend.clientCreates.get());
            Assert.assertArrayEquals(
                    new String[]{"127.0.0.1:5658"}, backend.createdNodes);
            Assert.assertEquals(1, backend.sendCalls.get());

            byte[] frame = backend.sentFrames.get(0);
            Assert.assertEquals(64, frame.length);
            Assert.assertEquals(ICommand.MAGIC, frame[0]);
            Assert.assertEquals(ICommand.VERSION, frame[1]);
            Assert.assertEquals(ICommand.COMMAND_TYPE_PING, frame[2]);
            Assert.assertEquals(120000L, backend.lastSendTimeout);
        } finally {
            client.close();
        }
    }

    @Test
    public void syncTimeoutMapsToClientCommandTimeoutException() {
        FakeBackend backend = new FakeBackend();
        backend.sendFailure = new IOException("command timeout");
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        try {
            client.ping();
            Assert.fail("expected a timeout");
        } catch (ClientCommandTimeoutException expected) {
            Assert.assertTrue(expected.getMessage().contains("timeout"));
        } catch (SlockException unexpected) {
            throw new AssertionError(unexpected);
        } finally {
            client.close();
        }
    }

    @Test
    public void lockCommandDeadlineIncludesTheLegacyCommandWindow() {
        LockCommand command = new LockCommand(
                ICommand.COMMAND_TYPE_LOCK, (byte) 0, new byte[16], null, 7, 0);
        Assert.assertEquals(127000L, NativeClient.deadlineMillis(command));
        Assert.assertEquals(120000L, NativeClient.deadlineMillis(new PingCommand()));
    }

    @Test
    public void writeFailureUsesTheLegacyOutputStreamException() {
        FakeBackend backend = new FakeBackend();
        backend.writeFailure = new IOException("broken pipe");
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        try {
            client.writeCommand(new PingCommand());
            Assert.fail("expected a write failure");
        } catch (ClientOutputStreamException expected) {
            Assert.assertTrue(expected.getMessage().contains("broken pipe"));
        } catch (SlockException unexpected) {
            throw new AssertionError(unexpected);
        } finally {
            client.close();
        }
    }

    @Test
    public void writeCommandForwardsTheCompleteFrameWithoutFakingASuccess() throws Exception {
        FakeBackend backend = new FakeBackend();
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        try {
            SlockDatabase database = client.selectDatabase((byte) 0);
            Assert.assertNotNull(database);
            client.open();
            client.writeCommand(new PingCommand());

            Assert.assertEquals(1, backend.writeCalls.get());
            Assert.assertEquals(64, backend.writtenFrames.get(0).length);
            Assert.assertEquals(ICommand.COMMAND_TYPE_PING, backend.writtenFrames.get(0)[2]);
        } finally {
            client.close();
        }
    }

    @Test
    public void asyncCommandRequiresAnEnabledCallbackPool() {
        FakeBackend backend = new FakeBackend();
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        try {
            client.sendCommand(new PingCommand(), result -> {
            });
            Assert.fail("expected the callback pool to be disabled");
        } catch (ClientAsyncCallbackDisabledException expected) {
            Assert.assertEquals(0, backend.submitCalls.get());
        } catch (SlockException unexpected) {
            throw new AssertionError(unexpected);
        } finally {
            client.close();
        }
    }

    @Test
    public void asyncSubmitFailureIsSynchronousAndDoesNotNotifyTheCallback() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.submitFailure = new IOException("client not connected");
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        AtomicInteger callbacks = new AtomicInteger();
        try {
            client.enableAsyncCallback();
            client.open();
            try {
                client.sendCommand(new PingCommand(), result -> callbacks.incrementAndGet());
                Assert.fail("expected a submit failure");
            } catch (ClientUnconnectException expected) {
                Assert.assertEquals(0, callbacks.get());
                Assert.assertEquals(0, backend.awaitCalls.get());
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void asyncCompletionFailureNotifiesTheCallbackExactlyOnce() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.awaitFailure = new IOException("command timeout");
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<CallbackCommandResult> delivered = new AtomicReference<>();
        try {
            client.enableAsyncCallback();
            client.open();
            client.sendCommand(new PingCommand(), result -> {
                callbacks.incrementAndGet();
                delivered.set(result);
                callbackLatch.countDown();
            });

            Assert.assertTrue(callbackLatch.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(1, callbacks.get());
            Assert.assertTrue(delivered.get().getException() instanceof ClientCommandTimeoutException);
            Assert.assertEquals(1, backend.releaseCalls.get());
        } finally {
            client.close();
        }
    }

    @Test
    public void asyncCompletionFallsBackToTheCompletionThreadWhenTheExecutorRejects() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.response = pingResponse();
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        CallbackExecutorManager manager = new CallbackExecutorManager(new RejectingExecutor());
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<CallbackCommandResult> delivered = new AtomicReference<>();
        try {
            client.enableAsyncCallback(manager);
            client.open();
            client.sendCommand(new PingCommand(), result -> {
                callbacks.incrementAndGet();
                delivered.set(result);
                callbackLatch.countDown();
            });

            Assert.assertTrue(callbackLatch.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(1, callbacks.get());
            Assert.assertNull(delivered.get().getException());
            Assert.assertEquals(1, backend.releaseCalls.get());
        } finally {
            client.close();
        }
    }

    @Test
    public void asyncCompletionStillNotifiesWhenTheClientIsClosedWhileAwaiting() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.holdCompletions = true;
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger callbacks = new AtomicInteger();
        try {
            client.enableAsyncCallback();
            client.open();
            client.sendCommand(new PingCommand(), result -> {
                callbacks.incrementAndGet();
                callbackLatch.countDown();
            });
            Assert.assertEquals(0, callbacks.get());
            Assert.assertEquals(0, backend.awaitCalls.get());

            long token = backend.submittedTokens.get(0);
            client.close();
            backend.complete(token, pingResponse());

            Assert.assertTrue(callbackLatch.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(1, callbacks.get());
        } finally {
            client.close();
        }
    }

    @Test
    public void pendingAsyncCompletionsUseOneBoundedPumpAndCompleteExactlyOnce() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.holdCompletions = true;
        ExecutorService executor = Executors.newFixedThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "facade-pump-callback");
            thread.setDaemon(true);
            return thread;
        });
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        int count = 64;
        CountDownLatch completed = new CountDownLatch(count);
        AtomicInteger callbacks = new AtomicInteger();
        try {
            client.enableAsyncCallback(new CallbackExecutorManager(executor));
            client.open();
            for (int i = 0; i < count; i++) {
                client.sendCommand(new PingCommand(), result -> {
                    callbacks.incrementAndGet();
                    completed.countDown();
                });
            }

            // Every command is accepted but held at the server: nothing ready
            // means nothing awaited, and no per-request wait thread is created.
            Assert.assertEquals(count, backend.submitCalls.get());
            Assert.assertEquals(0, backend.awaitCalls.get());
            Assert.assertEquals(0, callbacks.get());
            Assert.assertEquals(1, countLiveThreads("ruslock-raw-completion"));

            for (Long token : backend.submittedTokens) {
                backend.complete(token, pingResponse());
            }

            Assert.assertTrue(completed.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(count, callbacks.get());
            Assert.assertEquals(count, backend.awaitCalls.get());
            Assert.assertEquals(count, backend.releaseCalls.get());

            Thread.sleep(100);
            Assert.assertEquals(count, callbacks.get());
            Assert.assertEquals(1, countLiveThreads("ruslock-raw-completion"));
        } finally {
            client.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void heldCompletionFailureReachesTheCallbackExactlyOnce() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.holdCompletions = true;
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<CallbackCommandResult> delivered = new AtomicReference<>();
        try {
            client.enableAsyncCallback();
            client.open();
            client.sendCommand(new PingCommand(), result -> {
                callbacks.incrementAndGet();
                delivered.set(result);
                callbackLatch.countDown();
            });
            Assert.assertEquals(0, backend.awaitCalls.get());

            backend.fail(backend.submittedTokens.get(0), new IOException("command timeout"));

            Assert.assertTrue(callbackLatch.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(1, callbacks.get());
            Assert.assertTrue(delivered.get().getException()
                    instanceof ClientCommandTimeoutException);
            Assert.assertEquals(1, backend.awaitCalls.get());
            Assert.assertEquals(1, backend.releaseCalls.get());
        } finally {
            client.close();
        }
    }

    private static int countLiveThreads(String name) {
        int live = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && name.equals(thread.getName())) {
                live++;
            }
        }
        return live;
    }

    @Test
    public void asyncCallbackRunsOnTheConfiguredManagerExecutor() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.response = pingResponse();
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "facade-callback-thread");
            thread.setDaemon(true);
            return thread;
        });
        CallbackExecutorManager manager = new CallbackExecutorManager(executor);
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicReference<String> callbackThread = new AtomicReference<>();
        try {
            client.enableAsyncCallback(manager);
            client.open();
            client.sendCommand(new PingCommand(), result -> {
                callbackThread.set(Thread.currentThread().getName());
                callbackLatch.countDown();
            });

            Assert.assertTrue(callbackLatch.await(5, TimeUnit.SECONDS));
            Assert.assertEquals("facade-callback-thread", callbackThread.get());
        } finally {
            client.close();
        }
    }

    @Test
    public void databaseDispatchUsesTheConfiguredCallbackExecutor() throws Exception {
        FakeBackend backend = new FakeBackend();
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        SlockDatabase database = client.selectDatabase((byte) 0);
        CountDownLatch dispatched = new CountDownLatch(1);
        try {
            try {
                database.requireAsyncCallback();
                Assert.fail("expected the callback pool to be disabled");
            } catch (ClientAsyncCallbackDisabledException expected) {
                // expected
            }

            client.enableAsyncCallback();
            client.open();
            database.dispatch(dispatched::countDown);
            Assert.assertTrue(dispatched.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
        }
    }

    @Test
    public void defaultFlagsReachTheNativeDatabaseOnFirstUseOnly() throws Exception {
        FakeBackend backend = new FakeBackend();
        SlockClient client = new SlockClient(backend, "127.0.0.1", 5658);
        SlockDatabase database = client.selectDatabase((byte) 17);
        try {
            client.setDefaultTimeoutFlag((short) 0x1234);
            client.setDefaultExpriedFlag((short) 0x5678);

            Assert.assertEquals(0, backend.databaseCreates.get());
            Assert.assertTrue(backend.databaseFlags.isEmpty());

            long handle = database.nativeHandle();
            Assert.assertEquals(1, backend.databaseCreates.get());
            Assert.assertEquals(17, (int) backend.databaseIds.get(0));
            Assert.assertEquals(1, backend.databaseFlags.size());
            Assert.assertEquals(0x1234, backend.databaseFlags.get(0)[0]);
            Assert.assertEquals(0x5678, backend.databaseFlags.get(0)[1]);

            client.setDefaultTimeoutFlag((short) 0x4321);
            Assert.assertEquals(2, backend.databaseFlags.size());
            Assert.assertEquals(0x4321, backend.databaseFlags.get(1)[0]);
            Assert.assertEquals(0x5678, backend.databaseFlags.get(1)[1]);

            database.close();
            Assert.assertTrue(backend.releases.contains(handle));
            try {
                database.getClient();
                Assert.fail("expected the database close error");
            } catch (ClientClosedException expected) {
                Assert.assertEquals("database close", expected.getMessage());
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void replsetPassesTheWholeNodeListToTheNativeClient() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.response = pingResponse();
        String[] hosts = {"10.0.0.1:5658", "10.0.0.2:5658"};
        SlockReplsetClient client = new SlockReplsetClient(hosts, backend);
        try {
            client.open();

            Assert.assertEquals(1, backend.runtimeCreates.get());
            Assert.assertEquals(1, backend.clientCreates.get());
            Assert.assertArrayEquals(hosts, backend.createdNodes);
            Assert.assertTrue(client.tryOpen() == client);
            Assert.assertTrue(client.ping());

            List<SlockDatabase> databases = Collections.singletonList(
                    client.selectDatabase((byte) 5));
            long handle = databases.get(0).nativeHandle();
            Assert.assertTrue(handle != 0);
            Assert.assertEquals(5, (int) backend.databaseIds.get(0));
        } finally {
            client.close();
        }
    }

    @Test
    public void replsetOpenFailureIsReportedAsClientUnconnect() {
        FakeBackend backend = new FakeBackend();
        backend.openFailure = new IOException("connection refused");
        SlockReplsetClient client = new SlockReplsetClient(
                new String[]{"10.0.0.1:5658"}, backend);
        try {
            client.open();
            Assert.fail("expected the initial open to fail");
        } catch (ClientUnconnectException expected) {
            Assert.assertTrue(expected.getMessage().contains("refused"));
        } finally {
            client.close();
        }
    }

    @Test
    public void replsetTryOpenReturnsNullWhenNoNodeIsReachable() {
        FakeBackend backend = new FakeBackend();
        backend.tryOpenFailure = new IOException("not connected");
        SlockReplsetClient client = new SlockReplsetClient(
                new String[]{"10.0.0.1:5658"}, backend);
        try {
            Assert.assertNull(client.tryOpen());
        } finally {
            client.close();
        }
    }

    @Test
    public void replsetProtectedRequestMapIsCompatibilityOnly() {
        FakeBackend backend = new FakeBackend();
        SlockReplsetClient client = new SlockReplsetClient(
                new String[]{"10.0.0.1:5658"}, backend);
        try {
            Assert.assertNotNull(client.getRequests());
            Assert.assertFalse(client.hasLivedClient());
            Assert.assertFalse(client.doPendingRequestCommand(null, new PingCommand()));
            client.addLivedClient(null, true);
            client.removeLivedClient(null);
            client.addLivedLeaderClient(null);
            client.removeLivedLeaderClient(null);
            client.removePendingRequestCommand(new PingCommand());
            client.wakeupPendingRequestCommands(null);
            client.closePendingRequestCommands();
        } finally {
            client.close();
        }
    }

    private static byte[] pingResponse() {
        byte[] response = new byte[64];
        response[0] = ICommand.MAGIC;
        response[1] = ICommand.VERSION;
        response[2] = ICommand.COMMAND_TYPE_PING;
        response[19] = ICommand.COMMAND_RESULT_SUCCED;
        return response;
    }

    private static final class FakeBackend implements NativeClient.Backend {
        final AtomicInteger runtimeCreates = new AtomicInteger();
        final AtomicInteger clientCreates = new AtomicInteger();
        final AtomicInteger clientCloseCalls = new AtomicInteger();
        final AtomicInteger sendCalls = new AtomicInteger();
        final AtomicInteger writeCalls = new AtomicInteger();
        final AtomicInteger submitCalls = new AtomicInteger();
        final AtomicInteger awaitCalls = new AtomicInteger();
        final AtomicInteger releaseCalls = new AtomicInteger();
        final AtomicInteger databaseCreates = new AtomicInteger();
        final List<byte[]> sentFrames = new CopyOnWriteArrayList<>();
        final List<byte[]> writtenFrames = new CopyOnWriteArrayList<>();
        final List<Long> releases = new CopyOnWriteArrayList<>();
        final List<Integer> databaseIds = new CopyOnWriteArrayList<>();
        final List<int[]> databaseFlags = new CopyOnWriteArrayList<>();
        final List<Long> submittedTokens = new CopyOnWriteArrayList<>();
        final ConcurrentHashMap<Long, CompletionRecord> completions = new ConcurrentHashMap<>();

        volatile String[] createdNodes = new String[0];
        volatile byte[] response = new byte[64];
        volatile boolean holdCompletions;
        volatile IOException openFailure;
        volatile IOException tryOpenFailure;
        volatile IOException sendFailure;
        volatile IOException writeFailure;
        volatile IOException submitFailure;
        volatile IOException awaitFailure;
        volatile long lastSendTimeout;
        volatile int tryOpenResult = 1;
        private long nextHandle = 1000;
        private final AtomicLong nextCompletion = new AtomicLong(700);

        /**
         * Process-wide stand-in for the native completion generation. All fakes
         * share it, matching the single native epoch the real bridge exposes, so
         * one shared readiness pump can wait on any pending entry.
         */
        private static final Object EPOCH_MONITOR = new Object();
        private static long sharedEpoch;

        @Override
        public long runtimeCreate() {
            runtimeCreates.incrementAndGet();
            return 900;
        }

        @Override
        public long clientCreateNodes(long runtime, String[] nodes) {
            clientCreates.incrementAndGet();
            createdNodes = nodes.clone();
            return 901;
        }

        @Override
        public void clientOpen(long client) throws IOException {
            if (openFailure != null) {
                throw openFailure;
            }
        }

        @Override
        public int clientTryOpen(long client) throws IOException {
            if (tryOpenFailure != null) {
                throw tryOpenFailure;
            }
            return tryOpenResult;
        }

        @Override
        public void clientClose(long client) {
            clientCloseCalls.incrementAndGet();
        }

        @Override
        public boolean clientIsClosed(long client) {
            return false;
        }

        @Override
        public long databaseCreate(long client, int dbId) {
            databaseCreates.incrementAndGet();
            databaseIds.add(dbId);
            return nextHandle++;
        }

        @Override
        public void databaseSetDefaultFlags(long database, int timeoutFlags, int expiredFlags) {
            databaseFlags.add(new int[]{timeoutFlags, expiredFlags});
        }

        @Override
        public byte[] sendCommand(long client, byte[] frame, long timeoutMillis) throws IOException {
            sendCalls.incrementAndGet();
            sentFrames.add(frame.clone());
            lastSendTimeout = timeoutMillis;
            if (sendFailure != null) {
                throw sendFailure;
            }
            return response.clone();
        }

        @Override
        public void writeCommand(long client, byte[] frame) throws IOException {
            writeCalls.incrementAndGet();
            writtenFrames.add(frame.clone());
            if (writeFailure != null) {
                throw writeFailure;
            }
        }

        @Override
        public long sendRawSubmit(long client, byte[] frame, long timeoutMillis) throws IOException {
            submitCalls.incrementAndGet();
            sentFrames.add(frame.clone());
            if (submitFailure != null) {
                throw submitFailure;
            }
            long token = nextCompletion.incrementAndGet();
            CompletionRecord record = new CompletionRecord();
            record.response = response.clone();
            record.done.set(!holdCompletions);
            completions.put(token, record);
            submittedTokens.add(token);
            return token;
        }

        @Override
        public byte[] sendRawAwait(long completion) throws IOException {
            awaitCalls.incrementAndGet();
            CompletionRecord record = completions.get(completion);
            if (record != null && record.failure != null) {
                throw record.failure;
            }
            if (awaitFailure != null) {
                throw awaitFailure;
            }
            if (record != null && record.response != null) {
                return record.response.clone();
            }
            return response.clone();
        }

        @Override
        public boolean completionIsDone(long completion) {
            CompletionRecord record = completions.get(completion);
            // Tokens not created by this fake (e.g. the synchronous path) are
            // treated as already complete.
            return record == null || record.done.get();
        }

        @Override
        public long completionEpoch() {
            synchronized (EPOCH_MONITOR) {
                return sharedEpoch;
            }
        }

        @Override
        public long awaitCompletionChange(long observed, long timeoutMillis) {
            synchronized (EPOCH_MONITOR) {
                if (timeoutMillis < 0) {
                    while (sharedEpoch == observed) {
                        try {
                            EPOCH_MONITOR.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return sharedEpoch;
                        }
                    }
                    return sharedEpoch;
                }
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
                while (sharedEpoch == observed) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) {
                        break;
                    }
                    try {
                        EPOCH_MONITOR.wait(
                                TimeUnit.NANOSECONDS.toMillis(remaining),
                                (int) (remaining % 1000000L));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                return sharedEpoch;
            }
        }

        @Override
        public void signalCompletionPump() {
            bumpEpoch();
        }

        void complete(long token, byte[] payload) {
            CompletionRecord record = completions.get(token);
            if (record == null) {
                throw new AssertionError("unknown completion token " + token);
            }
            record.response = payload == null ? new byte[0] : payload.clone();
            record.done.set(true);
            bumpEpoch();
        }

        void fail(long token, IOException error) {
            CompletionRecord record = completions.get(token);
            if (record == null) {
                throw new AssertionError("unknown completion token " + token);
            }
            record.failure = error;
            record.done.set(true);
            bumpEpoch();
        }

        private static void bumpEpoch() {
            synchronized (EPOCH_MONITOR) {
                sharedEpoch++;
                EPOCH_MONITOR.notifyAll();
            }
        }

        @Override
        public boolean release(long handle) {
            releaseCalls.incrementAndGet();
            releases.add(handle);
            return true;
        }
    }

    private static final class CompletionRecord {
        final AtomicBoolean done = new AtomicBoolean(false);
        volatile byte[] response;
        volatile IOException failure;
    }

    private static final class RejectingExecutor extends AbstractExecutorService {
        @Override
        public void execute(Runnable command) {
            throw new RejectedExecutionException("rejected by test executor");
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }
}
