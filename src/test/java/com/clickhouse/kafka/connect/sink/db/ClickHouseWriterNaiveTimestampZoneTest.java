package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SQL Server (and Debezium's SQL Server connector) has no timezone option: a naive DATETIME
 * column that actually holds local wall-clock time is emitted as an instant that mislabels
 * those local digits as UTC (org.apache.kafka.connect.data.Timestamp / io.debezium.time.Timestamp).
 * naiveTimestampZone lets every timestamp column be reinterpreted in its true source zone
 * instead. No live ClickHouse instance needed - this only exercises RowBinary serialization.
 */
public class ClickHouseWriterNaiveTimestampZoneTest {

    private static ClickHouseWriter writerWithConfig(Map<String, String> props) throws Exception {
        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        Field cscField = ClickHouseWriter.class.getDeclaredField("csc");
        cscField.setAccessible(true);
        cscField.set(writer, new ClickHouseSinkConfig(props));
        return writer;
    }

    private static long writeAndReadEpochMillis(ClickHouseWriter writer, Column column, Date value) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.doWriteColValue(column, out, new Data(Schema.OPTIONAL_INT64_SCHEMA, value), false);
        return ByteBuffer.wrap(out.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    @Test
    public void reinterpretsNaiveTimestampInConfiguredZone() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.NAIVE_TIMESTAMP_ZONE, "Africa/Cairo");
        ClickHouseWriter writer = writerWithConfig(props);
        Column column = Column.extractColumn(newDescriptor("CreateDate", "DateTime64(3, 'Africa/Cairo')"));

        LocalDateTime cairoWallClock = LocalDateTime.of(2026, 8, 24, 14, 3, 11, 543_000_000);
        // What Debezium/Kafka Connect actually hands the connector: the Cairo wall-clock digits
        // epoch-ized as if they were already UTC (SqlServerValueConverters hardcodes UTC).
        Date mislabeledAsUtc = Date.from(cairoWallClock.toInstant(ZoneOffset.UTC));

        long writtenMillis = writeAndReadEpochMillis(writer, column, mislabeledAsUtc);

        Instant trueInstant = cairoWallClock.atZone(ZoneId.of("Africa/Cairo")).toInstant();
        assertEquals(trueInstant.toEpochMilli(), writtenMillis);
    }

    @Test
    public void leavesNaiveTimestampUnchangedWhenNotConfigured() throws Exception {
        ClickHouseWriter writer = writerWithConfig(Collections.emptyMap());
        Column column = Column.extractColumn(newDescriptor("CreateDate", "DateTime64(3, 'Africa/Cairo')"));

        Date value = Date.from(LocalDateTime.of(2026, 8, 24, 14, 3, 11, 543_000_000).toInstant(ZoneOffset.UTC));

        long writtenMillis = writeAndReadEpochMillis(writer, column, value);

        assertEquals(value.getTime(), writtenMillis);
    }
}
