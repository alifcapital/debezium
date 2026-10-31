/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.time.Conversions;

/**
 * @author Jiri Pechanec
 *
 */
public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        source = new SourceInfo(new PostgresConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        .with(PostgresConnectorConfig.DATABASE_NAME, "serverX")
                        .build()));
        source.update(Conversions.toInstantFromMicros(123_456_789L), new TableId("catalogNameX", "schemaNameX", "tableNameX"));
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }

    @Test
    @FixFor("DBZ-934")
    public void canHandleNullValues() {
        source.update(null, null, null, null, null, null);
    }

    @Test
    public void shouldHaveTimestamp() {
        assertThat(source.struct().getInt64("ts_ms")).isEqualTo(123_456L);
    }

    @Test
    public void schemaIsCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.postgresql.Source")
                .version(SchemaFactory.SOURCE_INFO_DEFAULT_SCHEMA_VERSION)
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", SchemaFactory.get().snapshotRecordSchema())
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("txId", Schema.OPTIONAL_INT64_SCHEMA)
                .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("xmin", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        VerifyRecord.assertConnectSchemasAreEqual(null, source.struct().schema(), schema);
    }

    @Test
    public void sequenceShouldBeJsonArrayOfLastCommitLsnAndLsn() {
        source.updateLastCommit(Lsn.valueOf(123L));
        source.update(Lsn.valueOf(456L), null, null, null, null, null);
        assertThat(source.sequence()).isEqualTo("[\"123\",\"456\"]");

        source.updateLastCommit(null);
        assertThat(source.sequence()).isEqualTo("[null,\"456\"]");

        source.update(null, null, null, null, null, null);
        assertThat(source.sequence()).isEqualTo("[null,null]");

        source.updateLastCommit(Lsn.valueOf(123L));
        source.update(null, null, null, null, null, null);
        assertThat(source.sequence()).isEqualTo("[\"123\",null]");
    }

    @Test
    public void sequenceShouldBeSameAsJsonWrittenByJackson() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final Long[] values = { null, 0L, 1L, 23_456_789L, 0x16_3002D50L, Long.MAX_VALUE, -1L, Long.MIN_VALUE };

        for (Long lastCommitLsn : values) {
            for (Long lsn : values) {
                source.updateLastCommit(Lsn.valueOf(lastCommitLsn));
                source.update(Lsn.valueOf(lsn), null, null, null, null, null);

                final String expected = mapper.writeValueAsString(Arrays.asList(
                        lastCommitLsn != null ? Long.toString(lastCommitLsn) : null,
                        lsn != null ? Long.toString(lsn) : null));
                assertThat(source.sequence()).as("lastCommitLsn=%s, lsn=%s", lastCommitLsn, lsn).isEqualTo(expected);
            }
        }
    }

    @Test
    public void jsonStringShouldBeEscapedLikeJacksonDoesIt() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();

        assertThat(jsonString(null)).isEqualTo(mapper.writeValueAsString(null));

        final String[] values = {
                "",
                "123",
                "-9223372036854775808",
                "a\"b",
                "back\\slash",
                "line\nbreak\r\n",
                "tab\tform\fback\b",
                "\u0000\u001f",
                "\u007f\u0080\u009f",
                "\u00fc\u00ef",
                "\u2028\u2029",
                "\ud83d\ude00",
                "\ud83d",
                "mixed \"quoted\"\\ and \u0001 text",
                "/",
                "</script>"
        };
        for (String value : values) {
            assertThat(jsonString(value)).as(value).isEqualTo(mapper.writeValueAsString(value));
        }

        // Every single char, so that no char that needs or does not need escaping is missed
        for (int c = Character.MIN_VALUE; c <= Character.MAX_VALUE; c++) {
            final String value = String.valueOf((char) c);
            assertThat(jsonString(value)).as("char %d", c).isEqualTo(mapper.writeValueAsString(value));
        }
    }

    private static String jsonString(String value) {
        final StringBuilder target = new StringBuilder();
        SourceInfo.appendJsonString(target, value);
        return target.toString();
    }
}
