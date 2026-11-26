package io.github.snower.jaslock;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Focused tests for the native resource loading contract. These tests never load a real
 * native library; the actual load is validated once the JNI artifact is available.
 */
public class NativeLibraryTest {

    @Test
    public void platformIdMapsSupportedOperatingSystemsAndArchitectures() {
        assertEquals("windows-x86_64", NativeLibrary.platformId("Windows 11", "amd64"));
        assertEquals("windows-x86_64", NativeLibrary.platformId("Windows 10", "x86_64"));
        assertEquals("linux-x86_64", NativeLibrary.platformId("Linux", "amd64"));
        assertEquals("linux-x86_64", NativeLibrary.platformId("Linux", "x86_64"));
        assertEquals("linux-aarch64", NativeLibrary.platformId("Linux", "aarch64"));
        assertEquals("linux-aarch64", NativeLibrary.platformId("Linux", "arm64"));
        assertEquals("macos-x86_64", NativeLibrary.platformId("Mac OS X", "x86_64"));
        assertEquals("macos-x86_64", NativeLibrary.platformId("Darwin", "amd64"));
        assertEquals("macos-aarch64", NativeLibrary.platformId("Mac OS X", "aarch64"));
        assertEquals("macos-aarch64", NativeLibrary.platformId("Mac OS X", "arm64"));
    }

    @Test
    public void platformIdRejectsUnsupportedSystemsWithActionableMessages() {
        try {
            NativeLibrary.platformId("SunOS", "x86_64");
            fail("expected unsupported operating system to be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("Unsupported operating system"));
            assertTrue(expected.getMessage().contains("SunOS"));
        }

        try {
            NativeLibrary.platformId("Linux", "x86");
            fail("expected unsupported architecture to be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("Unsupported CPU architecture"));
            assertTrue(expected.getMessage().contains("x86"));
        }
    }

    @Test
    public void libraryFileNameFollowsJvmPlatformNaming() {
        assertEquals("ruslock_java.dll", NativeLibrary.libraryFileName("windows-x86_64"));
        assertEquals("libruslock_java.so", NativeLibrary.libraryFileName("linux-x86_64"));
        assertEquals("libruslock_java.so", NativeLibrary.libraryFileName("linux-aarch64"));
        assertEquals("libruslock_java.dylib", NativeLibrary.libraryFileName("macos-x86_64"));
        assertEquals("libruslock_java.dylib", NativeLibrary.libraryFileName("macos-aarch64"));
    }

    @Test
    public void resourcePathShowsExactlyWhereTheBundledLibraryIsExpected() {
        assertEquals(
                "io/github/snower/jaslock/native/windows-x86_64/ruslock_java.dll",
                NativeLibrary.resourcePath("windows-x86_64"));
        assertEquals(
                "io/github/snower/jaslock/native/linux-aarch64/libruslock_java.so",
                NativeLibrary.resourcePath("linux-aarch64"));
    }

    @Test
    public void missingBundledResourceFailsWithPlatformAndResourcePath() {
        try {
            NativeLibrary.bundledResource("linux-x86_64", "missing-smoke-library.so");
            fail("expected missing bundled resource to be rejected");
        } catch (IllegalStateException expected) {
            String message = expected.getMessage();
            assertTrue(message.contains("linux-x86_64"));
            assertTrue(message.contains(
                    "io/github/snower/jaslock/native/linux-x86_64/missing-smoke-library.so"));
            assertTrue(message.contains("no java.library.path fallback"));
        }
    }

    @Test
    public void privateExtractionDirectoriesAreFreshIsolatedAndRestrictive() throws Exception {
        File tmp = Files.createTempDirectory("ruslock-native-test").toFile();
        try {
            File first = NativeLibrary.createPrivateExtractionDirectory(
                    tmp.getAbsolutePath(), "1.1.5", "windows-x86_64");
            File second = NativeLibrary.createPrivateExtractionDirectory(
                    tmp.getAbsolutePath(), "1.1.5", "windows-x86_64");

            assertTrue(first.isDirectory());
            assertTrue(second.isDirectory());
            assertNotEquals(first, second);
            assertTrue(first.getName().startsWith("ruslock-native-1.1.5-windows-x86_64-"));
            assertTrue(second.getName().startsWith("ruslock-native-1.1.5-windows-x86_64-"));
            assertEquals(tmp.getCanonicalFile(), first.getParentFile().getCanonicalFile());
            assertEquals(tmp.getCanonicalFile(), second.getParentFile().getCanonicalFile());

            if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
                Set<PosixFilePermission> ownerOnly = PosixFilePermissions.fromString("rwx------");
                assertEquals(ownerOnly, Files.getPosixFilePermissions(first.toPath()));
                assertEquals(ownerOnly, Files.getPosixFilePermissions(second.toPath()));
            }
        } finally {
            deleteRecursively(tmp);
        }
    }

    @Test
    public void preexistingPredictableExtractionPathIsNeverUsed() throws Exception {
        File tmp = Files.createTempDirectory("ruslock-native-test").toFile();
        byte[] attackerBytes = "attacker-controlled".getBytes(StandardCharsets.UTF_8);
        byte[] libraryBytes = "fresh-private-library".getBytes(StandardCharsets.UTF_8);
        try {
            File predictable = new File(
                    new File(new File(tmp, "ruslock-native"), "1.1.5"), "windows-x86_64");
            assertTrue(predictable.mkdirs());
            File sentinel = new File(predictable, "ruslock_java.dll");
            Files.write(sentinel.toPath(), attackerBytes);

            File fresh = NativeLibrary.createPrivateExtractionDirectory(
                    tmp.getAbsolutePath(), "1.1.5", "windows-x86_64");
            File target = new File(fresh, "ruslock_java.dll");
            NativeLibrary.extract(new ByteArrayInputStream(libraryBytes), target);

            assertArrayEquals(attackerBytes, Files.readAllBytes(sentinel.toPath()));
            assertArrayEquals(libraryBytes, Files.readAllBytes(target.toPath()));
            assertNotEquals(sentinel.getCanonicalPath(), target.getCanonicalPath());
            assertFalse(target.getCanonicalPath().startsWith(
                    predictable.getCanonicalPath() + File.separator));
        } finally {
            deleteRecursively(tmp);
        }
    }

    @Test
    public void extractionReplacesRatherThanReusingAPreexistingTargetFile() throws Exception {
        File tmp = Files.createTempDirectory("ruslock-native-test").toFile();
        byte[] staleBytes = "stale".getBytes(StandardCharsets.UTF_8);
        byte[] libraryBytes = "fresh".getBytes(StandardCharsets.UTF_8);
        try {
            File directory = NativeLibrary.createPrivateExtractionDirectory(
                    tmp.getAbsolutePath(), "1.1.5", "windows-x86_64");
            File target = new File(directory, "ruslock_java.dll");
            Files.write(target.toPath(), staleBytes);

            NativeLibrary.extract(new ByteArrayInputStream(libraryBytes), target);

            assertArrayEquals(libraryBytes, Files.readAllBytes(target.toPath()));
            File[] leftovers = directory.listFiles();
            assertEquals(1, leftovers.length);
            assertEquals(target, leftovers[0]);
        } finally {
            deleteRecursively(tmp);
        }
    }

    @Test
    public void unsafeVersionPrefixSegmentsCannotEscapeOrSelectTheSharedPath() throws Exception {
        File tmp = Files.createTempDirectory("ruslock-native-test").toFile();
        try {
            File extracted = NativeLibrary.createPrivateExtractionDirectory(
                    tmp.getAbsolutePath(), "../..", "linux-x86_64");

            assertTrue(extracted.getName().startsWith("ruslock-native-dev-linux-x86_64-"));
            assertEquals(tmp.getCanonicalFile(), extracted.getParentFile().getCanonicalFile());
        } finally {
            deleteRecursively(tmp);
        }
    }

    @Test
    public void extractionDirectoryRejectsAMissingTempDirectory() {
        try {
            NativeLibrary.createPrivateExtractionDirectory(
                    null, "1.1.5", "linux-x86_64");
            fail("expected a missing java.io.tmpdir to be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("java.io.tmpdir"));
        }
    }

    @Test
    public void extractionDirectoryRejectsATempDirectoryThatIsNotADirectory() throws Exception {
        File tmp = Files.createTempDirectory("ruslock-native-test").toFile();
        File missing = new File(tmp, "missing-tmp");
        try {
            NativeLibrary.createPrivateExtractionDirectory(
                    missing.getAbsolutePath(), "1.1.5", "linux-x86_64");
            fail("expected a non-directory java.io.tmpdir to be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("java.io.tmpdir"));
            assertTrue(expected.getMessage().contains("not an existing directory"));
        } finally {
            deleteRecursively(tmp);
        }
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        // Best effort cleanup: Windows may briefly hold a file.
        file.delete();
    }
}
