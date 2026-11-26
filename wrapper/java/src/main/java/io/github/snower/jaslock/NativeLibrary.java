package io.github.snower.jaslock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Locale;
import java.util.Set;

/**
 * Loads the platform-native RusLock JNI library that is packaged inside this artifact.
 *
 * <p>Each load creates a new private directory below the configured {@code java.io.tmpdir},
 * extracts the bundled resource there, and loads that fresh copy with
 * {@link System#load(String)}. Nothing is reused from a predictable or pre-existing path, and
 * the private directories are registered with {@link File#deleteOnExit()} (best effort; Windows
 * cannot delete a loaded DLL until the JVM exits, so the private directory may remain until OS
 * cleanup). There is deliberately no {@code System.loadLibrary} or {@code java.library.path}
 * fallback: a supported platform without a bundled artifact is a packaging error and must fail
 * with a clear message instead of silently selecting an unrelated local library.
 */
final class NativeLibrary {

    private static final String LIBRARY_BASE_NAME = "ruslock_java";
    private static final String RESOURCE_DIRECTORY = "native";
    private static final String RESOURCE_ROOT = "io/github/snower/jaslock/native";
    private static final String DIRECTORY_PREFIX = "ruslock-native-";
    private static final String DEFAULT_VERSION = "dev";
    private static final String PRIVATE_DIRECTORY_PERMISSIONS = "rwx------";
    private static final int COPY_BUFFER_SIZE = 16 * 1024;

    private static final Object LOAD_LOCK = new Object();
    private static boolean loaded;

    private NativeLibrary() {
    }

    /**
     * Loads the bundled native library exactly once for this class loader.
     *
     * @throws IllegalStateException if the platform is unsupported, the resource is missing,
     *                               extraction fails, or the JVM denies the load
     * @throws UnsatisfiedLinkError  if the extracted library cannot be loaded by the JVM
     */
    static void load() {
        synchronized (LOAD_LOCK) {
            if (loaded) {
                return;
            }

            String platform = platformId(
                    System.getProperty("os.name"), System.getProperty("os.arch"));
            String fileName = libraryFileName(platform);
            String path = resourcePath(platform);
            URL resource = bundledResource(platform, fileName);
            File directory = createPrivateExtractionDirectory(
                    System.getProperty("java.io.tmpdir"),
                    libraryVersion(),
                    platform);
            File target = new File(directory, fileName);
            boolean success = false;

            try {
                try (InputStream input = resource.openStream()) {
                    extract(input, target);
                }

                // Delete hooks run in reverse registration order: register the directory first
                // so the library file is attempted first on JVM exit.
                directory.deleteOnExit();
                target.deleteOnExit();

                System.load(target.getAbsolutePath());
                loaded = true;
                success = true;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to extract the RusLock native library for platform '" + platform
                                + "' from classpath resource '" + path
                                + "' into the private directory '" + directory + "'.", e);
            } catch (UnsatisfiedLinkError e) {
                UnsatisfiedLinkError failure = new UnsatisfiedLinkError(
                        "Failed to load the RusLock native library for platform '" + platform
                                + "' from the fresh private extraction '" + target + "'. Each"
                                + " load uses a new private copy, so this is not a shared"
                                + " extraction path or class loader collision. Verify that the"
                                + " artifact matches this JVM's operating system and CPU"
                                + " architecture and that its native dependencies are available."
                                + " The JVM reported: " + e.getMessage());
                failure.initCause(e);
                throw failure;
            } catch (SecurityException e) {
                throw new IllegalStateException(
                        "The security manager denied loading the RusLock native library for"
                                + " platform '" + platform + "' from '" + target + "'.", e);
            } finally {
                if (!success) {
                    cleanup(directory, target);
                }
            }
        }
    }

    static boolean isLoaded() {
        synchronized (LOAD_LOCK) {
            return loaded;
        }
    }

    /**
     * Maps JVM {@code os.name}/{@code os.arch} values to the resource directory key used by
     * the Maven build. Only platforms with a bundled artifact are accepted.
     */
    static String platformId(String osName, String osArch) {
        String os = normalizeOs(osName);
        if (os == null) {
            throw new IllegalStateException(
                    "Unsupported operating system for the RusLock native library: '" + osName
                            + "'. Supported platforms: windows-x86_64, linux-x86_64,"
                            + " linux-aarch64, macos-x86_64, macos-aarch64.");
        }

        String arch = normalizeArch(osArch);
        if (arch == null) {
            throw new IllegalStateException(
                    "Unsupported CPU architecture for the RusLock native library: '" + osArch
                            + "' on '" + osName + "'. Supported architectures: x86_64 (amd64),"
                            + " aarch64 (arm64).");
        }

        return os + "-" + arch;
    }

    /** Returns the platform-specific file name of the Rust JNI cdylib. */
    static String libraryFileName(String platform) {
        if (platform.startsWith("windows-")) {
            return LIBRARY_BASE_NAME + ".dll";
        }
        if (platform.startsWith("linux-")) {
            return "lib" + LIBRARY_BASE_NAME + ".so";
        }
        if (platform.startsWith("macos-")) {
            return "lib" + LIBRARY_BASE_NAME + ".dylib";
        }
        throw new IllegalArgumentException(
                "Unsupported RusLock native platform directory: '" + platform + "'.");
    }

    /** Returns the classpath resource path (without a leading slash) for a platform. */
    static String resourcePath(String platform) {
        return RESOURCE_ROOT + "/" + platform + "/" + libraryFileName(platform);
    }

    /**
     * Finds the bundled resource through this class's own class-relative resource lookup and
     * fails with the expected path when the artifact does not contain one for the running
     * platform.
     */
    static URL bundledResource(String platform, String fileName) {
        URL resource = NativeLibrary.class.getResource(
                RESOURCE_DIRECTORY + "/" + platform + "/" + fileName);
        if (resource == null) {
            String path = RESOURCE_ROOT + "/" + platform + "/" + fileName;
            throw new IllegalStateException(
                    "The RusLock native library is not bundled for platform '" + platform
                            + "'. Expected classpath resource '" + path + "'. This artifact was"
                            + " built without the native library for the running platform; no"
                            + " java.library.path fallback is attempted.");
        }
        return resource;
    }

    /**
     * Creates a new, unique extraction directory below the trusted {@code java.io.tmpdir}.
     * The directory name carries a sanitized version/platform prefix, and POSIX file systems
     * create it with owner-only {@code rwx------} permissions where the file store supports it.
     */
    static File createPrivateExtractionDirectory(String tmpDirectory, String version, String platform) {
        if (tmpDirectory == null || tmpDirectory.trim().isEmpty()) {
            throw new IllegalStateException(
                    "The java.io.tmpdir system property is not set; cannot create a private"
                            + " RusLock native extraction directory for platform '" + platform + "'.");
        }

        Path parent = Paths.get(tmpDirectory);
        if (!Files.isDirectory(parent)) {
            throw new IllegalStateException(
                    "The java.io.tmpdir location is not an existing directory: '" + tmpDirectory
                            + "'; cannot create a private RusLock native extraction directory.");
        }

        String prefix = DIRECTORY_PREFIX
                + sanitizeComponent(version, DEFAULT_VERSION) + "-"
                + sanitizeComponent(platform, DEFAULT_VERSION) + "-";

        try {
            if (Files.getFileStore(parent).supportsFileAttributeView("posix")) {
                Set<PosixFilePermission> permissions =
                        PosixFilePermissions.fromString(PRIVATE_DIRECTORY_PERMISSIONS);
                try {
                    return Files.createTempDirectory(
                            parent, prefix, PosixFilePermissions.asFileAttribute(permissions))
                            .toFile();
                } catch (UnsupportedOperationException e) {
                    // The file store advertised POSIX but rejected the attribute; fall through
                    // to the JVM's secure default for temporary directories.
                }
            }
            return Files.createTempDirectory(parent, prefix).toFile();
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to create a private RusLock native extraction directory under"
                            + " java.io.tmpdir '" + parent + "' for version '" + version
                            + "' and platform '" + platform + "'.", e);
        }
    }

    /**
     * Copies an input stream to {@code target} through a temporary file in the same private
     * directory and atomically installs it. A pre-existing target is replaced, never trusted
     * or reused.
     */
    static File extract(InputStream input, File target) throws IOException {
        if (target.getName().isEmpty()) {
            throw new IOException("Native library extraction target has no file name.");
        }
        File directory = target.getParentFile();
        if (directory == null || !directory.isDirectory()) {
            throw new IOException(
                    "Native library extraction directory does not exist: '" + directory + "'.");
        }

        File temporary = File.createTempFile(target.getName() + ".", ".tmp", directory);
        boolean installed = false;
        try {
            try (OutputStream output = new FileOutputStream(temporary)) {
                copy(input, output);
                output.flush();
            }

            try {
                Files.move(temporary.toPath(), target.toPath(),
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary.toPath(), target.toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
            }

            installed = true;
            setOwnerPermissions(target);
            return target;
        } finally {
            if (!installed && temporary.exists()) {
                // Best effort: a stale temporary file is harmless, a half-installed target is not.
                temporary.delete();
            }
        }
    }

    private static void cleanup(File directory, File target) {
        if (target != null) {
            target.delete();
        }
        if (directory != null) {
            File[] remaining = directory.listFiles();
            if (remaining != null) {
                for (File file : remaining) {
                    file.delete();
                }
            }
            directory.delete();
        }
    }

    private static String normalizeOs(String osName) {
        if (osName == null) {
            return null;
        }
        String value = osName.toLowerCase(Locale.ROOT);
        if (value.contains("linux")) {
            return "linux";
        }
        if (value.contains("darwin") || value.contains("mac")) {
            return "macos";
        }
        if (value.contains("win")) {
            return "windows";
        }
        return null;
    }

    private static String normalizeArch(String osArch) {
        if (osArch == null) {
            return null;
        }
        String value = osArch.trim().toLowerCase(Locale.ROOT);
        if ("amd64".equals(value) || "x86_64".equals(value) || "x86-64".equals(value)) {
            return "x86_64";
        }
        if ("aarch64".equals(value) || "arm64".equals(value)) {
            return "aarch64";
        }
        return null;
    }

    private static String libraryVersion() {
        Package libraryPackage = NativeLibrary.class.getPackage();
        String version = null;
        if (libraryPackage != null) {
            version = libraryPackage.getImplementationVersion();
            if (version == null || version.trim().isEmpty()) {
                version = libraryPackage.getSpecificationVersion();
            }
        }
        return sanitizeComponent(version, DEFAULT_VERSION);
    }

    private static String sanitizeComponent(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String sanitized = value.trim().replaceAll("[^A-Za-z0-9._-]", "_");
        if (sanitized.isEmpty()
                || ".".equals(sanitized)
                || "..".equals(sanitized)
                || sanitized.contains("..")) {
            return fallback;
        }
        return sanitized;
    }

    private static void setOwnerPermissions(File file) {
        // Best effort only: Windows and some file systems do not support POSIX permission bits.
        file.setReadable(true, true);
        file.setWritable(true, true);
        file.setExecutable(true, true);
    }

    private static void copy(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        int read;
        while ((read = input.read(buffer)) != -1) {
            output.write(buffer, 0, read);
        }
    }
}
