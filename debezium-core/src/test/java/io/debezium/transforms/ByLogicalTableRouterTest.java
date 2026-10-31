/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.relational.history.HistoryRecord.Fields;

/**
 * @author Mario Mueller
 */
public class ByLogicalTableRouterTest {

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("server1.public.transactions_20240101_20240131.Key")
            .field("id", Schema.INT64_SCHEMA)
            .build();

    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .optional()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .optional()
            .field("table", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    @Test(expected = ConnectException.class)
    public void testBrokenKeyReplacementConfigurationNullValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "$1");
        props.put("key.field.regex", "If this is set, key.field.replacement must be non-empty");
        subject.configure(props);
    }

    @Test(expected = ConnectException.class)
    public void testBrokenKeyReplacementConfigurationEmptyValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "$1");
        props.put("key.field.regex", "If this is set, key.field.replacement must be non-empty");
        props.put("key.field.replacement", "");
        subject.configure(props);
    }

    @Test
    public void testKeyReplacementWorkingConfigurationWithAvroSchemaNameAdjustment() {
        testKeyReplacementWorkingConfiguration("avro",
                "mysql_server_1.inventory.customers_all_shards.Key");
    }

    @Test
    @FixFor("DBZ-3535")
    public void testKeyReplacementWorkingConfigurationWithNoSchemaNameAdjustment() {
        testKeyReplacementWorkingConfiguration("none",
                "mysql-server-1.inventory.customers_all_shards.Key");
    }

    private void testKeyReplacementWorkingConfiguration(String schemaNameAdjustmentMode, String expectedKeySchemaName) {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*)customers_shard(.*)");
        props.put("topic.replacement", "$1customers_all_shards");
        props.put("key.field.name", "shard_id");
        props.put("key.field.regex", "(.*)customers_shard_(.*)");
        props.put("key.field.replacement", "$2");
        props.put("schema.name.adjustment.mode", schemaNameAdjustmentMode);
        router.configure(props);

        Schema keySchema = SchemaBuilder.struct()
                .name("mysql-server-1.inventory.customers_shard_1.Key")
                .field("id", SchemaBuilder.int64().build())
                .build();

        Struct key1 = new Struct(keySchema).put("id", 123L);

        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "mysql-server-1.inventory.customers_shard_1", keySchema, key1, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo("mysql-server-1.inventory.customers_all_shards");

        assertThat(transformed1.keySchema().name()).isEqualTo(expectedKeySchemaName);
        assertThat(transformed1.keySchema().fields()).hasSize(2);
        assertThat(transformed1.keySchema().fields().get(0).name()).isEqualTo("id");
        assertThat(transformed1.keySchema().fields().get(1).name()).isEqualTo("shard_id");

        assertThat(((Struct) transformed1.key()).get("id")).isEqualTo(123L);
        assertThat(((Struct) transformed1.key()).get("shard_id")).isEqualTo("1");

        Struct key2 = new Struct(keySchema).put("id", 123L);

        SourceRecord record2 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "mysql-server-1.inventory.customers_shard_2", keySchema, key2, null, null);

        SourceRecord transformed2 = router.apply(record2);
        assertThat(transformed2).isNotNull();
        assertThat(transformed2.topic()).isEqualTo("mysql-server-1.inventory.customers_all_shards");

        assertThat(transformed2.keySchema().name()).isEqualTo(expectedKeySchemaName);
        assertThat(transformed2.keySchema().fields()).hasSize(2);
        assertThat(transformed2.keySchema().fields().get(0).name()).isEqualTo("id");
        assertThat(transformed2.keySchema().fields().get(1).name()).isEqualTo("shard_id");

        assertThat(((Struct) transformed2.key()).get("id")).isEqualTo(123L);
        assertThat(((Struct) transformed2.key()).get("shard_id")).isEqualTo("2");

        Struct key3 = new Struct(keySchema).put("id", 456L);

        SourceRecord record3 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "mysql-server-1.inventory.customers_shard_2", keySchema, key3, null, null);

        SourceRecord transformed3 = router.apply(record3);
        assertThat(transformed3).isNotNull();
        assertThat(transformed3.topic()).isEqualTo("mysql-server-1.inventory.customers_all_shards");

        assertThat(transformed3.keySchema().name()).isEqualTo(expectedKeySchemaName);
        assertThat(transformed3.keySchema().fields()).hasSize(2);
        assertThat(transformed3.keySchema().fields().get(0).name()).isEqualTo("id");
        assertThat(transformed3.keySchema().fields().get(1).name()).isEqualTo("shard_id");

        assertThat(((Struct) transformed3.key()).get("id")).isEqualTo(456L);
        assertThat(((Struct) transformed3.key()).get("shard_id")).isEqualTo("2");
    }

    @Test
    public void testHeartbeatMessageTopicReplacementTopic() {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();
        final String keyFieldName = "serverName";
        final String keyOriginalKeyTopic = "originalTopic";
        final String replacedTopic = "debezium_heartbeat_central";

        props.put("topic.regex", "__debezium-heartbeat(.*)");
        props.put("topic.replacement", replacedTopic);
        props.put("key.field.name", keyOriginalKeyTopic);
        router.configure(props);

        Schema keySchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.ServerNameKey")
                .field(keyFieldName, Schema.STRING_SCHEMA)
                .build();

        Struct key1 = new Struct(keySchema).put(keyFieldName, "test_server_name_db");
        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "__debezium-heartbeat.test_server_name_db", keySchema, key1, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo(replacedTopic);

        assertThat(transformed1.keySchema().name()).isEqualTo(replacedTopic + ".Key");
        assertThat(transformed1.keySchema().fields()).hasSize(2);
        assertThat(transformed1.keySchema().fields().get(0).name()).isEqualTo(keyFieldName);
        assertThat(transformed1.keySchema().fields().get(1).name()).isEqualTo(keyOriginalKeyTopic);

        assertThat(((Struct) transformed1.key()).get(keyFieldName)).isEqualTo("test_server_name_db");
        assertThat(((Struct) transformed1.key()).get(keyOriginalKeyTopic)).isEqualTo("__debezium-heartbeat.test_server_name_db");

        assertThat(transformed1.value()).isNull();

        Struct key2 = new Struct(keySchema).put(keyFieldName, "test_server_name_db_2");
        SourceRecord record2 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "__debezium-heartbeat.test_server_name_db_2", keySchema, key2, null, null);

        SourceRecord transformed2 = router.apply(record2);
        assertThat(transformed2).isNotNull();
        assertThat(transformed2.topic()).isEqualTo(replacedTopic);

        assertThat(transformed2.keySchema().name()).isEqualTo(replacedTopic + ".Key");
        assertThat(transformed2.keySchema().fields()).hasSize(2);
        assertThat(transformed2.keySchema().fields().get(0).name()).isEqualTo(keyFieldName);
        assertThat(transformed2.keySchema().fields().get(1).name()).isEqualTo(keyOriginalKeyTopic);

        assertThat(((Struct) transformed2.key()).get(keyFieldName)).isEqualTo("test_server_name_db_2");
        assertThat(((Struct) transformed2.key()).get(keyOriginalKeyTopic)).isEqualTo("__debezium-heartbeat.test_server_name_db_2");

        assertThat(transformed2.value()).isNull();
    }

    @Test(expected = ConnectException.class)
    public void testBrokenTopicReplacementConfigurationNullValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        // topic.replacement is not set, therefore null. Must crash.
        props.put("topic.regex", "someValidRegex(.*)");
        subject.configure(props);
    }

    @Test(expected = ConnectException.class)
    public void testBrokenTopicReplacementConfigurationEmptyValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        // topic.replacement is set to empty string. Must crash.
        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "");
        subject.configure(props);
    }

    @Test
    @FixFor("DBZ-1086")
    public void testKeyNullValue() {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*)customers_shard(.*)");
        props.put("topic.replacement", "$1customers_all_shards");
        props.put("key.field.name", "shard_id");
        props.put("key.field.regex", "(.*)customers_shard_(.*)");
        props.put("key.field.replacement", "$2");
        router.configure(props);

        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "mysql-server-1.inventory.customers_shard_1", null, null, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo("mysql-server-1.inventory.customers_all_shards");

        assertThat(transformed1.keySchema()).isNull();
        assertThat(transformed1.key()).isNull();
    }

    @Test
    @FixFor("DBZ-2034")
    public void testNamespaceReplacementWithoutKeyChange() {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*).dbz_shard_\\d+.(.*)");
        props.put("topic.replacement", "dbz_core.$2");
        props.put("key.enforce.uniqueness", "false");
        router.configure(props);

        Schema keySchema = SchemaBuilder.struct()
                .name("s21.dbz_shard_21.address.Key")
                .field("id", SchemaBuilder.int64().build())
                .build();

        Struct key1 = new Struct(keySchema).put("id", 123L);

        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "s21.dbz_shard_21.address", keySchema, key1, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo("dbz_core.address"); // no change expected

        assertThat(transformed1.keySchema().name()).isEqualTo("dbz_core.address.Key");
        assertThat(transformed1.keySchema().fields()).hasSize(1);
        assertThat(transformed1.keySchema().fields().get(0).name()).isEqualTo("id");

        assertThat(((Struct) transformed1.key()).get("id")).isEqualTo(123L);

        Struct key2 = new Struct(keySchema).put("id", 123L);

        // This second record is basically the same as the first, and should _actually_ be indistinguishable
        keySchema = SchemaBuilder.struct()
                .name("s22.dbz_shard_22.address.Key")
                .field("id", SchemaBuilder.int64().build())
                .build();
        SourceRecord record2 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "s22.dbz_shard_22.address", keySchema, key2, null, null);

        SourceRecord transformed2 = router.apply(record2);
        assertThat(transformed2).isNotNull();
        assertThat(transformed2.topic()).isEqualTo("dbz_core.address");

        assertThat(transformed2.keySchema().name()).isEqualTo("dbz_core.address.Key");
        assertThat(transformed2.keySchema().fields()).hasSize(1);
        assertThat(transformed2.keySchema().fields().get(0).name()).isEqualTo("id");

        assertThat(((Struct) transformed2.key()).get("id")).isEqualTo(123L);
    }

    @Test
    @FixFor("DBZ-2412")
    public void shouldHandleSchemaChangeEvent() throws Exception {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();

        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*)");
        props.put("topic.replacement", "$1_rerouted");

        router.configure(props);

        Schema schemaChangeKeySchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.SchemaChangeKey")
                .field(Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                .build();

        // just a basic non-CDC envelope
        Schema schemaChangeValueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.SchemaChangeValue")
                .field(Fields.DATABASE_NAME, Schema.OPTIONAL_STRING_SCHEMA);

        final SourceRecord record = new SourceRecord(null, Collections.singletonMap("offset", "1"), "schema_changes", 0,
                schemaChangeKeySchema, new Struct(schemaChangeKeySchema).put(Fields.DATABASE_NAME, "my-db"), schemaChangeValueSchema,
                new Struct(schemaChangeValueSchema).put(Fields.DATABASE_NAME, "my-db"));

        SourceRecord transformed = router.apply(record);
        assertThat(transformed.topic()).isEqualTo("schema_changes_rerouted");
        assertThat(transformed.value()).isSameAs(record.value());
    }

    @Test
    public void shouldPassThroughTopicsThatDoNotMatchOnRepeatedCalls() {
        final ByLogicalTableRouter<SourceRecord> router = transactionsRouter(new HashMap<>());

        final SourceRecord other = createKeyedRecord("server1.public.accounts", 1L);
        final SourceRecord matching = createKeyedRecord("server1.public.transactions_20240101_20240131", 1L);

        for (int i = 0; i < 3; i++) {
            assertThat(router.apply(other)).isSameAs(other);

            final SourceRecord routed = router.apply(matching);
            assertThat(routed.topic()).isEqualTo("server1.public.transactions");
            assertThat(routed.keySchema().name()).isEqualTo("server1.public.transactions.Key");
            assertThat(((Struct) routed.key()).get("id")).isEqualTo(1L);
            assertThat(((Struct) routed.key()).get("__dbz__physicalTableIdentifier")).isEqualTo("server1.public.transactions_20240101_20240131");
        }
    }

    @Test
    public void shouldNotRunTopicRegexAgainForTopicsAlreadySeen() throws Exception {
        final ByLogicalTableRouter<SourceRecord> router = transactionsRouter(new HashMap<>());

        final SourceRecord other = createKeyedRecord("server1.public.accounts", 1L);
        final SourceRecord matching = createKeyedRecord("server1.public.transactions_2024_01", 1L);
        assertThat(router.apply(other)).isSameAs(other);
        assertThat(router.apply(matching).topic()).isEqualTo("server1.public.transactions");

        // Now the regex would give the opposite answer for both topics, so only a remembered answer can keep the result
        setRouterField(router, "topicRegex", Pattern.compile("server1\\.public\\.accounts"));
        setRouterField(router, "topicReplacement", "must.not.be.used");

        assertThat(router.apply(other)).isSameAs(other);
        assertThat(router.apply(matching).topic()).isEqualTo("server1.public.transactions");
    }

    @Test
    public void shouldKeepPassingThroughRecordWhenRegexGivesEmptyTopic() {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();
        props.put("topic.regex", "(.*)\\.(.*)");
        props.put("topic.replacement", "$2");
        router.configure(props);

        final SourceRecord record = createKeyedRecord("server1.", 1L);

        assertThat(router.apply(record)).isSameAs(record);
        assertThat(router.apply(record)).isSameAs(record);
    }

    @Test
    public void shouldRouteTombstoneWithRewrittenKey() {
        final ByLogicalTableRouter<SourceRecord> router = transactionsRouter(new HashMap<>());
        final String oldTopic = "server1.public.transactions_20240101_20240131";

        final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), oldTopic, 3,
                KEY_SCHEMA, new Struct(KEY_SCHEMA).put("id", 7L), null, null, 42L);

        final SourceRecord routed = router.apply(tombstone);

        assertThat(routed.topic()).isEqualTo("server1.public.transactions");
        assertThat(routed.kafkaPartition()).isEqualTo(3);
        assertThat(routed.timestamp()).isEqualTo(42L);
        assertThat(routed.keySchema()).isEqualTo(expectedKeySchema(true));
        assertThat(routed.key()).isEqualTo(expectedKey(7L, oldTopic));
        assertThat(routed.valueSchema()).isNull();
        assertThat(routed.value()).isNull();
    }

    @Test
    public void shouldRouteCreateUpdateAndDeleteEvents() {
        final ByLogicalTableRouter<SourceRecord> router = transactionsRouter(new HashMap<>());
        final String oldTopic = "server1.public.transactions_20240101_20240131";
        final Instant timestamp = Instant.ofEpochMilli(1_700_000_000_123L);

        final Envelope oldEnvelope = envelope(oldTopic, VALUE_SCHEMA);
        final Envelope newEnvelope = envelope("server1.public.transactions", expectedValueSchema());
        final Struct source = new Struct(SOURCE_SCHEMA).put("table", "transactions_20240101_20240131");

        // Repeated to also cover the path where the new schemas come out of the caches
        for (int i = 0; i < 3; i++) {
            final Struct key = new Struct(KEY_SCHEMA).put("id", 1L);

            final SourceRecord created = router.apply(valueRecord(oldTopic, key, oldEnvelope,
                    oldEnvelope.create(value(VALUE_SCHEMA, 1L, "alice", 10), source, timestamp)));
            assertRouted(created, 1L, oldTopic,
                    newEnvelope.create(value(expectedValueSchema(), 1L, "alice", 10), source, timestamp));

            final SourceRecord updated = router.apply(valueRecord(oldTopic, key, oldEnvelope,
                    oldEnvelope.update(value(VALUE_SCHEMA, 1L, "alice", 10), value(VALUE_SCHEMA, 1L, "alice", 20), source, timestamp)));
            assertRouted(updated, 1L, oldTopic,
                    newEnvelope.update(value(expectedValueSchema(), 1L, "alice", 10), value(expectedValueSchema(), 1L, "alice", 20), source, timestamp));

            final SourceRecord deleted = router.apply(valueRecord(oldTopic, key, oldEnvelope,
                    oldEnvelope.delete(value(VALUE_SCHEMA, 1L, "alice", 20), source, timestamp)));
            assertRouted(deleted, 1L, oldTopic,
                    newEnvelope.delete(value(expectedValueSchema(), 1L, "alice", 20), source, timestamp));
        }
    }

    @Test
    public void shouldRouteEventsWithNullFieldsAndWithoutKeyUniqueness() {
        final Map<String, String> props = new HashMap<>();
        props.put("key.enforce.uniqueness", "false");
        final ByLogicalTableRouter<SourceRecord> router = transactionsRouter(props);
        final String oldTopic = "server1.public.transactions_2024_01";
        final Instant timestamp = Instant.ofEpochMilli(1_700_000_000_123L);

        final Envelope oldEnvelope = envelope(oldTopic, VALUE_SCHEMA);
        final Envelope newEnvelope = envelope("server1.public.transactions", expectedValueSchema());

        final SourceRecord created = router.apply(valueRecord(oldTopic, new Struct(KEY_SCHEMA).put("id", 5L), oldEnvelope,
                oldEnvelope.create(value(VALUE_SCHEMA, 5L, null, null), null, timestamp)));

        assertThat(created.topic()).isEqualTo("server1.public.transactions");
        assertThat(created.keySchema()).isEqualTo(expectedKeySchema(false));
        assertThat(created.key()).isEqualTo(new Struct(expectedKeySchema(false)).put("id", 5L));
        assertThat(created.valueSchema()).isEqualTo(newEnvelope.schema());
        assertThat(created.value()).isEqualTo(newEnvelope.create(value(expectedValueSchema(), 5L, null, null), null, timestamp));
    }

    @Test
    public void shouldUseKeyFieldRegexForPhysicalTableIdentifier() {
        final Map<String, String> props = new HashMap<>();
        props.put("key.field.name", "shard");
        props.put("key.field.regex", ".*transactions_(.*)");
        props.put("key.field.replacement", "$1");
        final ByLogicalTableRouter<SourceRecord> router = transactionsRouter(props);

        final SourceRecord routed = router.apply(createKeyedRecord("server1.public.transactions_2024_01", 9L));

        final Schema expectedKeySchema = SchemaBuilder.struct()
                .name("server1.public.transactions.Key")
                .field("id", Schema.INT64_SCHEMA)
                .field("shard", Schema.STRING_SCHEMA)
                .build();
        assertThat(routed.keySchema()).isEqualTo(expectedKeySchema);
        assertThat(routed.key()).isEqualTo(new Struct(expectedKeySchema).put("id", 9L).put("shard", "2024_01"));
    }

    @Test
    public void shouldCopyFieldsByNameWhenBeforeAndAfterListThemInDifferentOrder() {
        final ByLogicalTableRouter<SourceRecord> router = transactionsRouter(new HashMap<>());
        final String oldTopic = "server1.public.transactions_2024_01";

        final Schema afterSchema = SchemaBuilder.struct()
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema oldEnvelopeSchema = SchemaBuilder.struct()
                .name(oldTopic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, VALUE_SCHEMA)
                .field(Envelope.FieldName.AFTER, afterSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .optional()
                .build();
        final Struct oldEnvelope = new Struct(oldEnvelopeSchema)
                .put(Envelope.FieldName.BEFORE, value(VALUE_SCHEMA, 1L, "alice", 10))
                .put(Envelope.FieldName.AFTER, new Struct(afterSchema).put("id", 1L).put("name", "alice").put("amount", 20))
                .put(Envelope.FieldName.OPERATION, "u");

        final SourceRecord routed = router.apply(valueRecord(oldTopic, new Struct(KEY_SCHEMA).put("id", 1L), new Envelope(oldEnvelopeSchema), oldEnvelope));

        final Schema newValueSchema = expectedValueSchema();
        final Struct routedEnvelope = (Struct) routed.value();
        assertThat(routedEnvelope.get(Envelope.FieldName.BEFORE)).isEqualTo(value(newValueSchema, 1L, "alice", 10));
        assertThat(routedEnvelope.get(Envelope.FieldName.AFTER)).isEqualTo(value(newValueSchema, 1L, "alice", 20));
        assertThat(routedEnvelope.get(Envelope.FieldName.OPERATION)).isEqualTo("u");
    }

    @Test
    public void shouldFailOnFieldThatIsMissingInNewSchema() {
        final ByLogicalTableRouter<SourceRecord> router = transactionsRouter(new HashMap<>());
        final String oldTopic = "server1.public.transactions_2024_01";

        final Schema afterSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .field("extra", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Schema oldEnvelopeSchema = SchemaBuilder.struct()
                .name(oldTopic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, VALUE_SCHEMA)
                .field(Envelope.FieldName.AFTER, afterSchema)
                .optional()
                .build();
        final Struct oldEnvelope = new Struct(oldEnvelopeSchema)
                .put(Envelope.FieldName.AFTER, new Struct(afterSchema).put("id", 1L).put("extra", "x"));

        assertThatThrownBy(() -> router.apply(valueRecord(oldTopic, new Struct(KEY_SCHEMA).put("id", 1L), new Envelope(oldEnvelopeSchema), oldEnvelope)))
                .isInstanceOf(DataException.class);
    }

    private static ByLogicalTableRouter<SourceRecord> transactionsRouter(Map<String, String> extraProps) {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>(extraProps);
        props.put("topic.regex", "^(.*)\\.(.*)\\.transactions(?:_\\d{8}_\\d{8}|_\\d{4}_\\d{2})?$");
        props.put("topic.replacement", "$1.$2.transactions");
        router.configure(props);
        return router;
    }

    private static void setRouterField(ByLogicalTableRouter<?> router, String name, Object value) throws Exception {
        final java.lang.reflect.Field field = ByLogicalTableRouter.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(router, value);
    }

    private static SourceRecord createKeyedRecord(String topic, long id) {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), topic, KEY_SCHEMA, new Struct(KEY_SCHEMA).put("id", id), null, null);
    }

    private static SourceRecord valueRecord(String topic, Struct key, Envelope envelope, Struct value) {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), topic, 1, key.schema(), key, envelope.schema(), value, 1_700_000_000_123L);
    }

    private static Envelope envelope(String topic, Schema valueSchema) {
        return Envelope.defineSchema()
                .withName(topic + ".Envelope")
                .withRecord(valueSchema)
                .withSource(SOURCE_SCHEMA)
                .build();
    }

    private static Struct value(Schema schema, long id, String name, Integer amount) {
        return new Struct(schema).put("id", id).put("name", name).put("amount", amount);
    }

    private static Schema expectedValueSchema() {
        return SchemaBuilder.struct()
                .name("server1.public.transactions.Value")
                .optional()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
    }

    private static Schema expectedKeySchema(boolean withTableIdentifier) {
        final SchemaBuilder builder = SchemaBuilder.struct()
                .name("server1.public.transactions.Key")
                .field("id", Schema.INT64_SCHEMA);
        if (withTableIdentifier) {
            builder.field("__dbz__physicalTableIdentifier", Schema.STRING_SCHEMA);
        }
        return builder.build();
    }

    private static Struct expectedKey(long id, String oldTopic) {
        return new Struct(expectedKeySchema(true)).put("id", id).put("__dbz__physicalTableIdentifier", oldTopic);
    }

    private static void assertRouted(SourceRecord routed, long id, String oldTopic, Struct expectedValue) {
        assertThat(routed.topic()).isEqualTo("server1.public.transactions");
        assertThat(routed.kafkaPartition()).isEqualTo(1);
        assertThat(routed.timestamp()).isEqualTo(1_700_000_000_123L);
        assertThat(routed.keySchema()).isEqualTo(expectedKeySchema(true));
        assertThat(routed.key()).isEqualTo(expectedKey(id, oldTopic));
        assertThat(routed.valueSchema()).isEqualTo(expectedValue.schema());
        assertThat(routed.value()).isEqualTo(expectedValue);
    }

    // FIXME: This SMT can use more tests for more detailed coverage.
    // The creation of a DBZ-ish SourceRecord is required for each test
}
