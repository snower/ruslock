# Java artifact version alignment

The Maven artifact `io.github.snower:ruslock` follows the root Cargo package version, currently `0.1.0`. Jaslock `1.1.5` remains the compatibility baseline; its tests and expected API signatures do not change.

Keep the version explicit in the published Maven POM, and check equality in Java CI. Generating the POM from Cargo would avoid a duplicated declaration but would complicate publishing and Maven consumption. Consumers and CI should use the wrapper project version rather than hard-coded JAR filenames or separate version literals. This does not change the Rust version, Java API, or native behavior.
