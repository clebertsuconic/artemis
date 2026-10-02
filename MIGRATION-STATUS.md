# Derby → H2 Migration Status

**Status:** PARKED — blocker in BLOB handling  
**Date:** 2026-10-02

## What's done

All find-and-replace work is complete (~33 files):

- **Maven:** `org.apache.derby:derby` → `com.h2database:h2:2.5.252` (test scope)
- **Driver:** `org.apache.derby.jdbc.EmbeddedDriver` → `org.h2.Driver`
- **URLs:** `jdbc:derby:memory:` → `jdbc:h2:mem:` (with `DB_CLOSE_DELAY=-1`)
- **Defaults:** User `SA`, password `""` (same as Derby)
- **Method renames:** `dropDerby` → `dropH2Database`, `shutdownDerby` → `shutdownH2`, etc.
- **DBSupportUtil:** `DROP ALL OBJECTS` (H2 equivalent of Derby's drop-by-URL)
- **SQL properties:** `journal-sql.properties` entries keyed `.h2` instead of `.derby`
- **Maven profiles:** `DB-derby-tests` → `DB-h2-tests`, `derby.load` → `h2.load`
- **Docs:** `persistence.adoc`, `database-store.txt`, `README.md` updated
- **Shutdown hook:** Removed from `JDBCConnectionProvider` production code (handle in tests)
- **PropertySQLProvider:** `HSQL` dialect entry kept for external HSQL users; `H2` already existed

## Blocker

`JDBCSequentialFileFactoryDriver.writeToFile()` uses updatable ResultSets to modify BLOBs:

```java
Blob blob = rs.getBlob(1);
bytesWritten = blob.setBytes(blob.length() + 1, data);
rs.updateBlob(1, blob);
rs.updateRow();
```

H2 doesn't support this pattern — `writeToFile()` silently returns 0 bytes written.

### What was tried
1. Adding `ID` to SELECT: `SELECT DATA, ID FROM %s WHERE ID=? FOR UPDATE` (like MySQL) — still fails
2. Removing `FOR UPDATE` (conflicts with `CONCUR_UPDATABLE` in H2) — still fails
3. The issue is fundamental: H2's `rs.updateRow()` doesn't persist BLOB changes made via `blob.setBytes()`

### Likely fix
Create an **H2-specific sequential file driver** (like `Db2SequentialFileDriver` or `PostgresSequentialSequentialFileDriver`) that uses direct `UPDATE ... SET DATA=? WHERE ID=?` statements instead of updatable ResultSets for BLOB writes.

Reference implementations:
- `Db2SequentialFileDriver` — uses `UPDATE %s SET DATA = (DATA || ?) WHERE ID=?` for append
- `PostgresSequentialSequentialFileDriver` — uses PostgreSQL Large Objects API

## Key H2 differences from Derby

| Aspect | Derby | H2 |
|--------|-------|----|
| In-memory lifetime | Until `jdbc:derby:;shutdown=true` | Until last connection closes (need `DB_CLOSE_DELAY=-1`) |
| BLOB via updatable ResultSet | Supported | **Not supported** (silently fails) |
| Drop database | `jdbc:derby:<name>;drop=true` | `DROP ALL OBJECTS` SQL |
| Shutdown | Special JDBC URL | `SHUTDOWN` SQL |
| License | Apache 2.0 | MPL 2.0 / EPL 1.0 (Category B — OK for test deps) |

## Test to validate fix

```bash
mvn -pl artemis-jdbc-store -Ptests -DfailIfNoTests=false \
    -Dtest=JDBCSequentialFileFactoryTest test
```

The `testReadZeroBytesOnNotEmptyFile` and `testReadOutOfBoundsOnNotEmptyFile` tests fail because `writeDirect()` → `writeToFile()` returns 0 bytes.

## Files changed

Key files (full list via `git diff --stat`):

- `artemis-jdbc-store/src/main/resources/journal-sql.properties` — H2-specific SQL entries
- `artemis-jdbc-store/src/main/java/.../JDBCConnectionProvider.java` — shutdown hook removed
- `artemis-unit-test-support/src/main/java/.../DBSupportUtil.java` — renamed methods
- `artemis-core-client/src/main/java/.../ActiveMQDefaultConfiguration.java` — default driver
- `tests/artemis-test-support/src/main/java/.../ActiveMQTestBase.java` — URLs, driver, detection
- `artemis-server/src/test/java/.../ServerTestBase.java` — same pattern
- `tests/db-tests/src/test/java/.../Database.java` — `H2("h2")` enum entry
- `pom.xml` — `h2.version=2.5.252`
- `artemis-pom/pom.xml` — dependency management
- 9 module POMs — `com.h2database:h2` test dep
- XML/JSON/YAML test configs — URLs and driver classes
- `docs/user-manual/persistence.adoc` — documentation
