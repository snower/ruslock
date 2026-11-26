package io.github.snower.consumer;

import io.github.snower.jaslock.Lock;
import io.github.snower.jaslock.SlockClient;
import io.github.snower.jaslock.exceptions.LockTimeoutException;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Dependency-only consumer fixture for the packaged RusLock artifact.
 *
 * <p>This class intentionally uses only the public {@code io.github.snower.jaslock} API. It
 * asserts that:
 *
 * <ul>
 *   <li>{@code SlockClient} came from the installed JAR (not a source/build directory),
 *   <li>{@code java.library.path} is empty and the native library was extracted by the
 *       packaged loader from {@code java.io.tmpdir},
 *   <li>a real Slock instance grants and releases a lock, and a second client cannot acquire
 *       the same held lock.
 * </ul>
 *
 * <p>It prints a single {@code CONSUMER: PASS} line on success. Any other outcome exits
 * non-zero so {@code verify-consumer.sh} fails closed.
 */
public final class ConsumerCheck {

    private static final String PASS = "CONSUMER: PASS";

    private ConsumerCheck() {
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5658;
        try {
            run(host, port);
            System.out.println(PASS);
        } catch (Throwable error) {
            System.out.println("CONSUMER: FAIL: " + error);
            error.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static void run(String host, int port) throws Exception {
        String expectedJar = requiredProperty("ruslock.expected.jar");
        String forbiddenRoot = System.getProperty("ruslock.forbidden.root", "");
        String libraryPath = System.getProperty("java.library.path");
        String tmpProperty = System.getProperty("java.io.tmpdir");

        if (libraryPath == null || !libraryPath.isEmpty()) {
            fail("java.library.path must be empty for this check, but was [" + libraryPath + "]");
        }
        if (tmpProperty == null || tmpProperty.trim().isEmpty()) {
            fail("java.io.tmpdir is not set; cannot verify the packaged extraction directory");
        }

        File tmpDirectory = new File(tmpProperty).getCanonicalFile();
        if (!tmpDirectory.isDirectory() && !tmpDirectory.mkdirs()) {
            fail("cannot create java.io.tmpdir for the consumer check: " + tmpDirectory);
        }

        File artifact = new File(expectedJar).getCanonicalFile();
        if (!artifact.isFile()) {
            fail("installed RusLock artifact not found: " + artifact);
        }

        verifyClassOrigin(artifact);
        verifyClasspathHasNoRepositorySources(forbiddenRoot, tmpDirectory);

        System.out.println("CONSUMER: jar=" + artifact.getPath());
        System.out.println("CONSUMER: jar-sha256=" + sha256(artifact));
        System.out.println("CONSUMER: java.library.path=empty");

        if (Boolean.parseBoolean(
                System.getProperty("ruslock.expect.missing.native", "false"))) {
            assertMissingNativeIsRejected(host, port);
            System.out.println("CONSUMER: missing-native-rejected=ok");
            return;
        }

        SlockClient owner = new SlockClient(host, port);
        owner.open();
        SlockClient contender = new SlockClient(host, port);
        contender.open();
        try {
            if (!owner.ping()) {
                fail("Slock ping returned false");
            }
            System.out.println("CONSUMER: ping=ok");

            reportExtraction(tmpDirectory);

            String key = "ruslock-consumer-" + UUID.randomUUID();
            Lock lock = owner.newLock(key, 5, 5);
            lock.acquire();
            lock.release();
            lock.acquire();
            lock.release();

            try (AutoCloseable ignored = owner.newLock(key, 5, 5).with()) {
                // Acquired and released by with().
            }
            System.out.println("CONSUMER: lock-roundtrip=ok");

            Lock held = owner.newLock(key, 5, 5);
            held.acquire();
            Lock contenderLock = contender.newLock(key, 1, 5);
            boolean blocked = false;
            try {
                contenderLock.acquire();
            } catch (LockTimeoutException expected) {
                blocked = true;
            }
            if (!blocked) {
                try {
                    contenderLock.release();
                } catch (Throwable ignored) {
                    // Best effort: the process is about to fail anyway.
                }
                fail("a second client acquired a lock held by the first client");
            }
            held.release();
            System.out.println("CONSUMER: lock-exclusion=LockTimeoutException");

            Lock afterRelease = contender.newLock(key, 5, 5);
            afterRelease.acquire();
            afterRelease.release();
            System.out.println("CONSUMER: lock-after-release=ok");
        } finally {
            contender.close();
            owner.close();
        }
    }

    /**
     * Negative control: a JAR without the platform resource must fail closed, naming the
     * expected resource and the no-fallback rule. If {@code open()} succeeds, an external
     * search path or a silent fallback is active and the check fails.
     */
    private static void assertMissingNativeIsRejected(String host, int port) {
        SlockClient client = null;
        Throwable failure = null;
        try {
            client = new SlockClient(host, port);
            client.open();
        } catch (Throwable error) {
            failure = error;
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Throwable ignored) {
                    // Best effort: no native handle was created.
                }
            }
        }

        if (failure == null) {
            fail("opened a client from a JAR without a bundled native library;"
                    + " a fallback path is active");
        }
        String chain = messageChain(failure);
        if (!chain.contains("not bundled")
                || !chain.contains("no java.library.path fallback")) {
            fail("missing-native failure did not name the absent resource and the"
                    + " no-fallback rule:\n" + chain);
        }
        String oneLine = chain.replace('\n', ' ').replace('\r', ' ').trim();
        if (oneLine.length() > 500) {
            oneLine = oneLine.substring(0, 500) + "...";
        }
        System.out.println("CONSUMER: missing-native-message=" + oneLine);
    }

    private static String messageChain(Throwable error) {
        StringBuilder chain = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            chain.append(current.getClass().getName());
            String message = current.getMessage();
            if (message != null && !message.isEmpty()) {
                chain.append(": ").append(message);
            }
            chain.append('\n');
            current = current.getCause();
        }
        return chain.toString();
    }

    /** Asserts that the facade class was loaded from the JAR installed in the isolated repo. */
    private static void verifyClassOrigin(File expectedJar) throws Exception {
        CodeSource source = SlockClient.class.getProtectionDomain().getCodeSource();
        if (source == null || source.getLocation() == null) {
            fail("SlockClient has no code source; cannot prove dependency-only loading");
        }
        File loadedFrom = new File(source.getLocation().toURI()).getCanonicalFile();
        if (!loadedFrom.isFile()) {
            fail("SlockClient was not loaded from a JAR: " + loadedFrom);
        }
        if (!loadedFrom.equals(expectedJar)) {
            fail("SlockClient was loaded from " + loadedFrom
                    + " instead of the installed artifact " + expectedJar);
        }
    }

    /**
     * Rejects classpath and temp entries that came from the wrapper source tree. This keeps the
     * check honest when someone runs the fixture inside the repository instead of the copied,
     * dependency-only project.
     */
    private static void verifyClasspathHasNoRepositorySources(String forbiddenRoot, File tmpDirectory)
            throws Exception {
        if (forbiddenRoot == null || forbiddenRoot.trim().isEmpty()) {
            return;
        }
        File root = new File(forbiddenRoot).getCanonicalFile();
        if (isUnder(tmpDirectory, root)) {
            fail("java.io.tmpdir is inside the repository source tree: " + tmpDirectory);
        }
        String classpath = System.getProperty("java.class.path", "");
        for (String entry : classpath.split(Pattern.quote(File.pathSeparator))) {
            if (entry.isEmpty()) {
                continue;
            }
            File file = new File(entry).getCanonicalFile();
            if (isUnder(file, root)) {
                fail("classpath entry came from the repository source tree: " + file);
            }
        }
    }

    /** Asserts that the native library was extracted into the configured private temp area. */
    private static void reportExtraction(File tmpDirectory) {
        File[] children = tmpDirectory.listFiles();
        File extraction = null;
        if (children != null) {
            for (File child : children) {
                if (!child.isDirectory() || !child.getName().startsWith("ruslock-native-")) {
                    continue;
                }
                File[] files = child.listFiles();
                if (files == null) {
                    continue;
                }
                for (File file : files) {
                    if (isNativeLibraryName(file.getName())) {
                        extraction = child;
                        break;
                    }
                }
                if (extraction != null) {
                    break;
                }
            }
        }
        if (extraction == null) {
            fail("no ruslock-native-* extraction containing a native library under " + tmpDirectory);
        }
        System.out.println("CONSUMER: native-extraction=" + extraction.getPath());
    }

    private static boolean isNativeLibraryName(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.endsWith(".dll") || lower.endsWith(".so") || lower.endsWith(".dylib");
    }

    private static boolean isUnder(File file, File root) {
        String path = file.getPath();
        String rootPath = root.getPath();
        return path.equals(rootPath) || path.startsWith(rootPath + File.separator);
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            fail("required system property is missing: " + name);
        }
        return value;
    }

    private static String sha256(File file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = new FileInputStream(file)) {
            byte[] buffer = new byte[16 * 1024];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        StringBuilder hex = new StringBuilder();
        for (byte value : digest.digest()) {
            hex.append(String.format("%02x", value & 0xff));
        }
        return hex.toString();
    }

    private static void fail(String message) {
        throw new IllegalStateException(message);
    }
}
