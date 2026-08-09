package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Debezium CDC envelope records into flat Records suitable for
 * insertion into a ClickHouse ReplacingMergeTree(_version, is_deleted) table.
 *
 * Detection: activated when the SinkRecord value schema name ends with ".Envelope".
 *
 * Routing:
 *   op = "c" / "r" / "u" → after struct, is_deleted = 0
 *   op = "d"             → before struct, is_deleted = 1
 *   op = "t"             → skip (truncate not supported)
 *
 * Injected columns:
 *   _version   UInt64 (PostgreSQL lsn / sequence, MySQL gtid / pos) or UInt256
 *              (SQL Server: (commit_lsn << 80) | change_lsn, each LSN packed losslessly —
 *              see extractVersion() and packSqlServerLsn())
 *   is_deleted UInt8        — 0 for upserts, 1 for deletes
 *   __ts_ms    DateTime64(3) — source.ts_ms, the commit time in the source database.
 *                             Enables end-to-end latency measurement (source commit → ClickHouse).
 *                             Omitted when the envelope carries no source.ts_ms.
 *
 * Type conversions (mirrors Altinity ClickHouseDataTypeMapper):
 *   STRING (no logical type)                 → String (pass-through)
 *   STRING + ZonedTimestamp                  → String (ISO8601 normalized)
 *   INT64  + MicroTimestamp / Timestamp      → String datetime
 *   INT32  + Date                            → java.sql.Date
 *   BYTES  + Decimal                         → BigDecimal
 *   BYTES  (no logical type)                 → String (hex)
 *   STRING (numeric, no logical type)        → String kept, BigDecimal attempted in writer
 */
public class DebeziumRecordConvertor extends RecordConvertor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumRecordConvertor.class);

    private static final String FIELD_OP     = "op";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER  = "after";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_LSN        = "lsn";
    private static final String FIELD_SEQUENCE   = "sequence";     // PostgreSQL snapshot: "[lastCommitLsn, lsn]"
    private static final String FIELD_GTID       = "gtid";
    private static final String FIELD_POS        = "pos";
    private static final String FIELD_CHANGE_LSN  = "change_lsn";   // SQL Server streaming
    private static final String FIELD_COMMIT_LSN  = "commit_lsn";   // SQL Server snapshot + streaming fallback
    private static final String FIELD_TS_MS      = "ts_ms";

    private static final String OP_DELETE   = "d";
    private static final String OP_TRUNCATE = "t";

    private static final String COL_VERSION    = "_version";
    private static final String COL_IS_DELETED = "is_deleted";
    private static final String COL_TS_MS      = "__ts_ms";

    // source.ts_ms is epoch millis. Wrapping it in the Connect Timestamp logical schema makes
    // ClickHouseWriter.doWriteDates() rescale it per the column's precision, so a DateTime64 target
    // is correct at any precision, and makes auto.evolve infer DateTime64(3) for the new column.
    private static final Schema TS_MS_SCHEMA = org.apache.kafka.connect.data.Timestamp.builder().optional().build();

    // Debezium logical type names
    private static final String LOGICAL_MICRO_TIMESTAMP  = "io.debezium.time.MicroTimestamp";
    private static final String LOGICAL_TIMESTAMP        = "org.apache.kafka.connect.data.Timestamp";
    private static final String LOGICAL_DATE             = "io.debezium.time.Date";
    private static final String LOGICAL_DECIMAL          = "org.apache.kafka.connect.data.Decimal";

    @Override
    public Record doConvert(SinkRecord sinkRecord, String topic, String database) {
        Struct envelope = (Struct) sinkRecord.value();
        String op = envelope.getString(FIELD_OP);

        if (op == null || op.equalsIgnoreCase(OP_TRUNCATE)) {
            LOGGER.debug("Skipping Debezium record with op={} for topic={}", op, topic);
            return Record.newRecord(SchemaType.DEBEZIUM_CDC,
                    topic, sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(),
                    Collections.emptyList(), Collections.emptyMap(), database, sinkRecord);
        }

        boolean isDelete = op.equalsIgnoreCase(OP_DELETE);
        Struct dataStruct = isDelete
                ? envelope.getStruct(FIELD_BEFORE)
                : envelope.getStruct(FIELD_AFTER);

        if (dataStruct == null) {
            LOGGER.warn("Debezium record op={} has null {} struct — skipping. topic={}",
                    op, isDelete ? FIELD_BEFORE : FIELD_AFTER, topic);
            return Record.newRecord(SchemaType.DEBEZIUM_CDC,
                    topic, sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(),
                    Collections.emptyList(), Collections.emptyMap(), database, sinkRecord);
        }

        Map<String, Data> jsonMap = toDebeziumJsonMap(dataStruct);

        Struct source = readSourceStruct(envelope, topic);

        BigInteger version = extractVersion(source);
        jsonMap.put(COL_VERSION,    new Data(Schema.BYTES_SCHEMA, version));
        jsonMap.put(COL_IS_DELETED, new Data(Schema.INT8_SCHEMA, isDelete ? (byte) 1 : (byte) 0));

        // Include synthetic fields in the fields list so validateDataSchema() can look them up.
        List<Field> fields = new ArrayList<>(dataStruct.schema().fields());
        fields.add(new Field(COL_VERSION,    fields.size(), SchemaBuilder.bytes().build()));
        fields.add(new Field(COL_IS_DELETED, fields.size(), SchemaBuilder.int8().build()));

        // Omitted rather than defaulted when absent, so a NULL means "no source commit time"
        // instead of reporting a 1970 epoch as a ~56-year latency.
        Long sourceTsMs = extractSourceTsMs(source);
        if (sourceTsMs != null) {
            jsonMap.put(COL_TS_MS, new Data(TS_MS_SCHEMA, new java.util.Date(sourceTsMs)));
            fields.add(new Field(COL_TS_MS, fields.size(), TS_MS_SCHEMA));
        }

        return Record.newRecord(SchemaType.DEBEZIUM_CDC,
                topic, sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(),
                fields, jsonMap, database, sinkRecord);
    }

    /**
     * Converts a Debezium after/before Struct into Map<String, Data> with
     * type-aware conversion for each Debezium logical type — mirrors the
     * conversion logic in Altinity's ClickHouseDataTypeMapper.
     */
    private Map<String, Data> toDebeziumJsonMap(Struct struct) {
        Map<String, Data> result = new HashMap<>();
        for (Field field : struct.schema().fields()) {
            String name       = field.name();
            Schema schema     = field.schema();
            Schema.Type type  = schema.type();
            String logicalName = schema.name();
            Object raw        = struct.get(field);

            if (raw == null) {
                result.put(name, new Data(schema, null));
                continue;
            }

            try {
                result.put(name, new Data(schema, convertValue(type, logicalName, raw)));
            } catch (Exception e) {
                LOGGER.warn("Could not convert field [{}] type={} logical={} — keeping raw value. Error: {}",
                        name, type, logicalName, e.getMessage());
                result.put(name, new Data(schema, raw));
            }
        }
        return result;
    }

    /**
     * Converts a single field value from its Debezium Java type to the Java type
     * that ClickHouseWriter.doWriteColValue() expects for each ClickHouse column type.
     */
    private Object convertValue(Schema.Type type, String logicalName, Object raw) {
        switch (type) {
            case BYTES:
                // Decimal logical type → BigDecimal
                if (LOGICAL_DECIMAL.equals(logicalName)) {
                    if (raw instanceof BigDecimal) return raw;
                    if (raw instanceof byte[])      return new BigDecimal(new BigInteger((byte[]) raw));
                    if (raw instanceof ByteBuffer) {
                        ByteBuffer buf = (ByteBuffer) raw;
                        byte[] bytes = new byte[buf.remaining()];
                        buf.get(bytes);
                        buf.rewind();
                        return new BigDecimal(new BigInteger(bytes));
                    }
                    return new BigDecimal(raw.toString());
                }
                // Raw bytes → hex string
                if (raw instanceof byte[])    return bytesToHex((byte[]) raw);
                if (raw instanceof ByteBuffer) {
                    ByteBuffer buf = (ByteBuffer) raw;
                    byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);
                    buf.rewind();
                    return bytesToHex(bytes);
                }
                return raw;

            case INT64:
                // doWriteDates handles Date natively in the INT64 branch.
                // For MicroTimestamp, normalize to microseconds Long so DateTime64(6) gets the right unit.
                if (LOGICAL_MICRO_TIMESTAMP.equals(logicalName)) {
                    if (raw instanceof java.util.Date) {
                        return ((java.util.Date) raw).getTime() * 1_000L;
                    }
                    return raw; // already Long microseconds
                }
                // For Timestamp, normalize to Date so doWriteDates uses doWriteDate(stream, date, precision).
                if (LOGICAL_TIMESTAMP.equals(logicalName)) {
                    if (raw instanceof java.util.Date) return raw;
                    return new java.util.Date(((Number) raw).longValue());
                }
                return raw;

            case INT32:
                // Days since epoch → java.sql.Date
                if (LOGICAL_DATE.equals(logicalName)) {
                    return java.sql.Date.valueOf(
                            java.time.LocalDate.ofEpochDay(((Number) raw).longValue()));
                }
                return raw;

            case STRING:
                // ZonedTimestamp is already a valid ISO8601 string — pass through as-is.
                // doWriteDates handles string input for DateTime64 columns.
                return raw;

            default:
                return raw;
        }
    }

    /**
     * Parses a PostgreSQL Debezium {@code source.sequence} string, e.g. {@code ["11793116936608","11793116937360"]},
     * and returns the last non-null element as a BigInteger. This is the read-position LSN (same address space as
     * streaming {@code source.lsn}). Returns null if no numeric element is present.
     */
    private static BigInteger parseSequenceLsn(String sequence) {
        String trimmed = sequence.trim();
        if (trimmed.length() < 2 || trimmed.charAt(0) != '[') return null;
        // Strip brackets, split on commas, walk from the end for the last numeric (non-null) token.
        String inner = trimmed.substring(1, trimmed.lastIndexOf(']') < 0 ? trimmed.length() : trimmed.lastIndexOf(']'));
        String[] parts = inner.split(",");
        for (int i = parts.length - 1; i >= 0; i--) {
            String token = parts[i].trim().replace("\"", "");
            if (token.isEmpty() || token.equalsIgnoreCase("null")) continue;
            try {
                return new BigInteger(token);
            } catch (NumberFormatException nfe) {
                return null;
            }
        }
        return null;
    }

    // SQL Server's LSN is a fixed 10-byte (80-bit) value: 4-byte VLF sequence number +
    // 4-byte log block ID + 2-byte slot number. A UInt256 _version column gives two of these
    // (commit_lsn and change_lsn) 128 bits of headroom each — comfortably more than the 80 any
    // single LSN needs — so every component is packed losslessly, with nothing to truncate.
    private static final int SQLSERVER_LSN_BITS = 80;
    private static final int SQLSERVER_LSN_BLOCK_SHIFT = 16;  // slot number occupies bits 0-15
    private static final int SQLSERVER_LSN_VLF_SHIFT   = 48;  // log block ID occupies bits 16-47

    /**
     * Parses a SQL Server LSN ("00000027:00000ac8:0003") into its true 80-bit value by shifting
     * each component into its own field width. Every component keeps full precision, so unlike a
     * fixed-mask packing no field can overflow its budget.
     *
     * Each component is parsed as a number rather than concatenating the hex text: Debezium always
     * zero-pads to 8:8:4, but text concatenation silently yields a completely different value for
     * the same LSN if that padding ever varies, which would corrupt version ordering rather than
     * fail loudly.
     */
    private static BigInteger packSqlServerLsn(String lsn) {
        String[] parts = lsn.split(":");
        if (parts.length != 3) return BigInteger.ZERO;
        BigInteger vlf   = new BigInteger(parts[0].trim(), 16);
        BigInteger block = new BigInteger(parts[1].trim(), 16);
        BigInteger slot  = new BigInteger(parts[2].trim(), 16);
        return vlf.shiftLeft(SQLSERVER_LSN_VLF_SHIFT)
                .or(block.shiftLeft(SQLSERVER_LSN_BLOCK_SHIFT))
                .or(slot);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /**
     * Reads the source struct once for both version and commit-time extraction. A malformed envelope
     * without a source field would otherwise throw a DataException out of doConvert and, under
     * errors.tolerance=none, kill the task; degrade to null so the record still writes.
     */
    private Struct readSourceStruct(Struct envelope, String topic) {
        try {
            return envelope.getStruct(FIELD_SOURCE);
        } catch (Exception e) {
            LOGGER.warn("Debezium envelope has no readable '{}' struct for topic={}: {}",
                    FIELD_SOURCE, topic, e.getMessage());
            return null;
        }
    }

    /**
     * Extracts the source database commit time (source.ts_ms) as epoch millis, or null when absent.
     * This is the change's commit time in the source DB — not envelope.ts_ms, which is when Debezium
     * read it — so it is the correct anchor for end-to-end latency.
     */
    private Long extractSourceTsMs(Struct source) {
        if (source == null) return null;

        try {
            Object tsMs = source.get(FIELD_TS_MS);
            // Number, not Long: JsonConverter narrows small int64 values to Integer.
            if (tsMs instanceof Number) return ((Number) tsMs).longValue();
        } catch (Exception e) {
            LOGGER.debug("Could not read source.ts_ms — field absent from envelope: {}", e.getMessage());
        }

        return null;
    }

    /**
     * Extracts the replication position from the Debezium source struct.
     * Priority: PostgreSQL LSN → MySQL GTID sequence number → MySQL binlog pos.
     */
    private BigInteger extractVersion(Struct source) {
        if (source == null) return BigInteger.ZERO;

        // PostgreSQL streaming: lsn is a Long
        try {
            Object lsn = source.get(FIELD_LSN);
            if (lsn instanceof Number) return BigInteger.valueOf(((Number) lsn).longValue());
        } catch (Exception e) {
            LOGGER.debug("Could not read source.lsn — not a PostgreSQL source or field absent: {}", e.getMessage());
        }

        // PostgreSQL snapshot (op=r): source.lsn is null, but source.sequence carries the LSN.
        // Debezium's SourceInfo.sequence() emits "[lastCommitLsn, lsn]" — both are Lsn.asLong()
        // values in the same address space as streaming source.lsn, so they are directly comparable.
        // Take the last non-null element (the read-position lsn) so a later streaming event always wins.
        try {
            Object sequence = source.get(FIELD_SEQUENCE);
            if (sequence instanceof String) {
                BigInteger seqVersion = parseSequenceLsn((String) sequence);
                if (seqVersion != null) return seqVersion;
            }
        } catch (Exception e) {
            LOGGER.debug("Could not parse source.sequence: {}", e.getMessage());
        }

        // MySQL: gtid = "uuid:N" — extract the sequence number N
        try {
            Object gtid = source.get(FIELD_GTID);
            if (gtid instanceof String) {
                String[] parts = ((String) gtid).split(":");
                if (parts.length == 2) return BigInteger.valueOf(Long.parseLong(parts[1].trim()));
            }
        } catch (Exception e) {
            LOGGER.debug("Could not parse source.gtid — not a MySQL source or unexpected format: {}", e.getMessage());
        }

        // MySQL fallback: binlog position
        try {
            Object pos = source.get(FIELD_POS);
            if (pos instanceof Number) return BigInteger.valueOf(((Number) pos).longValue());
        } catch (Exception e) {
            LOGGER.debug("Could not read source.pos — not a MySQL source or field absent: {}", e.getMessage());
        }

        // SQL Server: composite version = (commit_lsn << 64) | change_lsn
        // commit_lsn is always present; change_lsn is null during snapshot (op=r).
        // Higher commit_lsn always wins; within same commit, higher change_lsn wins.
        try {
            BigInteger commitPacked = BigInteger.ZERO;
            boolean hasCommit = false;
            Object commitLsn = source.get(FIELD_COMMIT_LSN);
            if (commitLsn instanceof String) {
                commitPacked = packSqlServerLsn((String) commitLsn);
                hasCommit = true;
            }

            BigInteger changePacked = BigInteger.ZERO;
            boolean hasChange = false;
            Object changeLsn = source.get(FIELD_CHANGE_LSN);
            if (changeLsn instanceof String) {
                changePacked = packSqlServerLsn((String) changeLsn);
                hasChange = true;
            }

            if (hasCommit || hasChange) {
                // Both halves are non-negative by construction (parsed from hex digit strings),
                // so this composite — up to 160 bits — needs a UInt256 _version column.
                return commitPacked.shiftLeft(SQLSERVER_LSN_BITS).or(changePacked);
            }
        } catch (Exception e) {
            LOGGER.debug("Could not parse SQL Server LSN fields: {}", e.getMessage());
        }

        LOGGER.warn("Could not extract version from Debezium source struct — defaulting to 0");
        return BigInteger.ZERO;
    }
}
