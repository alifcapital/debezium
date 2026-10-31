/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.MessageDecoderContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Checks when the pgoutput decoder reads column metadata from the database for a relation message.
 * The decoder gets binary pgoutput messages; the database connection, the schema and the type registry are mocks.
 */
public class PgOutputMessageDecoderTest {

    private static final int RELATION_ID = 16384;
    private static final TableId TABLE_ID = new TableId(null, "public", "orders");
    private static final Duration TTL = Duration.ofMinutes(10);

    private static final int INT4_OID = 23;
    private static final int TEXT_OID = 25;
    private static final PostgresType INT4 = new PostgresType.Builder(null, "int4", INT4_OID, Types.INTEGER, TypeRegistry.NO_TYPE_MODIFIER, null).build();
    private static final PostgresType TEXT = new PostgresType.Builder(null, "text", TEXT_OID, Types.VARCHAR, TypeRegistry.NO_TYPE_MODIFIER, null).build();

    private static final byte KEY_FLAG = 1;

    private PostgresConnection connection;
    private PostgresSchema schema;
    private TypeRegistry typeRegistry;
    private final Map<Integer, Table> appliedTables = new HashMap<>();
    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final List<ReplicationMessage> processed = new ArrayList<>();
    private final List<Table> tablesAtProcessing = new ArrayList<>();

    // Columns that the mocked database returns: "req" is NOT NULL until a test makes it nullable
    private List<Column> databaseColumns;

    @Before
    public void beforeEach() throws Exception {
        databaseColumns = columns(false);

        connection = mock(PostgresConnection.class);
        final Connection jdbcConnection = mock(Connection.class);
        when(connection.connection()).thenReturn(jdbcConnection);
        when(jdbcConnection.getMetaData()).thenReturn(mock(DatabaseMetaData.class));
        when(connection.getTableColumnsForDecoder(eq(TABLE_ID), any())).thenAnswer(invocation -> databaseColumns);
        when(connection.readPrimaryKeyNames(any(), eq(TABLE_ID))).thenAnswer(invocation -> new ArrayList<>(List.of("id")));

        schema = mock(PostgresSchema.class);
        doAnswer(invocation -> {
            appliedTables.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(schema).applySchemaChangesForTable(anyInt(), any());
        when(schema.tableFor(anyInt())).thenAnswer(invocation -> appliedTables.get((Integer) invocation.getArgument(0)));

        typeRegistry = mock(TypeRegistry.class);
        when(typeRegistry.get(INT4_OID)).thenReturn(INT4);
        when(typeRegistry.get(TEXT_OID)).thenReturn(TEXT);
        when(typeRegistry.get("int4")).thenReturn(INT4);
        when(typeRegistry.get("text")).thenReturn(TEXT);
    }

    @Test
    public void shouldReadMetadataOnceForRepeatedRelationMessage() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, relation());

        assertMetadataReads(1);
        verify(schema, times(2)).applySchemaChangesForTable(eq(RELATION_ID), any());
        assertThat(appliedTables.get(RELATION_ID).primaryKeyColumnNames()).containsExactly("id");
        assertThat(appliedTables.get(RELATION_ID).columnWithName("req").isOptional()).isFalse();
    }

    @Test
    public void shouldReadMetadataForEveryRelationMessageWhenCacheIsDisabled() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(Duration.ZERO);

        process(decoder, relation());
        process(decoder, relation());

        assertMetadataReads(2);
    }

    @Test
    public void shouldReadMetadataWhenRelationMessageChanges() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        databaseColumns = new ArrayList<>(columns(false));
        databaseColumns.add(Column.editor().name("note").optional(true).create());
        process(decoder, relation(column("note", TEXT_OID)));

        assertMetadataReads(2);
        assertThat(appliedTables.get(RELATION_ID).columnWithName("note")).isNotNull();
    }

    @Test
    public void shouldReadMetadataWhenCacheEntryExpires() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        now.addAndGet(TTL.toMillis() - 1);
        process(decoder, relation());
        assertMetadataReads(1);

        // A change that the relation message does not show is applied after the entry expires
        databaseColumns = columns(true);
        now.addAndGet(1);
        process(decoder, relation());
        assertMetadataReads(2);
        assertThat(appliedTables.get(RELATION_ID).columnWithName("req").isOptional()).isTrue();
    }

    @Test
    public void shouldReadMetadataBeforeInsertWithNullInRequiredColumn() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, relation());
        // DROP NOT NULL does not change the relation message, so the cache still marks "req" as required
        databaseColumns = columns(true);
        process(decoder, insert(tuple("1", "a", null)));

        assertMetadataReads(2);
        assertThat(tablesAtProcessing.get(0).columnWithName("req").isOptional()).isTrue();
        assertThat(values(processed.get(0).getNewTupleList())).containsExactly(1, "a", null);
    }

    @Test
    public void shouldNotReadMetadataBeforeInsertWithoutNullInRequiredColumn() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, relation());
        process(decoder, insert(tuple("1", null, "5")));

        assertMetadataReads(1);
    }

    @Test
    public void shouldNotReadMetadataAgainWhenItWasReadForTheRelationMessage() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        // The metadata is fresh, so a null in a required column is not a sign of a stale cache
        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", null)));

        assertMetadataReads(1);
    }

    @Test
    public void shouldReadMetadataOnceForRepeatedNullsInRequiredColumn() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", null)));
        process(decoder, insert(tuple("2", "b", null)));

        assertMetadataReads(2);
    }

    @Test
    public void shouldReadMetadataBeforeUpdateWithNullInRequiredColumnOfNewTuple() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, relation());
        databaseColumns = columns(true);
        process(decoder, update(null, null, tuple("1", "a", null)));

        assertMetadataReads(2);
        assertThat(tablesAtProcessing.get(0).columnWithName("req").isOptional()).isTrue();
    }

    @Test
    public void shouldReadMetadataBeforeUpdateWithNullInRequiredColumnOfOldTuple() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, relation());
        databaseColumns = columns(true);
        process(decoder, update('O', tuple("1", "a", null), tuple("1", "b", "5")));

        assertMetadataReads(2);
        final ReplicationMessage message = processed.get(0);
        assertThat(values(message.getOldTupleList())).containsExactly(1, "a", null);
        assertThat(values(message.getNewTupleList())).containsExactly(1, "b", 5);
    }

    @Test
    public void shouldIgnoreNullsOfKeyTuple() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, relation());
        // A key tuple has null in every column outside the replica identity
        process(decoder, update('K', tuple("1", null, null), tuple("2", "a", "5")));
        process(decoder, delete('K', tuple("2", null, null)));

        assertMetadataReads(1);
        assertThat(processed).hasSize(2);
        assertThat(values(processed.get(0).getNewTupleList())).containsExactly(2, "a", 5);
        assertThat(values(processed.get(1).getOldTupleList())).containsExactly(2, null, null);
    }

    @Test
    public void shouldReadMetadataBeforeDeleteWithNullInRequiredColumnOfOldTuple() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, relation());
        databaseColumns = columns(true);
        process(decoder, delete('O', tuple("1", "a", null)));

        assertMetadataReads(2);
        assertThat(tablesAtProcessing.get(0).columnWithName("req").isOptional()).isTrue();
    }

    @Test
    public void shouldResolveColumnTypesOncePerRelation() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", "5")));
        process(decoder, insert(tuple("2", null, "6")));
        process(decoder, update('O', tuple("2", null, "6"), tuple("2", "b", "6")));
        process(decoder, delete('O', tuple("2", "b", "6")));

        // "int4" is the type of two columns and each column asks the registry once
        verify(typeRegistry, times(2)).get("int4");
        verify(typeRegistry, times(1)).get("text");
        assertThat(processed).hasSize(4);
        for (ReplicationMessage message : processed) {
            final List<ReplicationMessage.Column> tuple = message.getOperation() == ReplicationMessage.Operation.DELETE
                    ? message.getOldTupleList()
                    : message.getNewTupleList();
            assertThat(types(tuple)).containsExactly(INT4, TEXT, INT4);
        }
        assertThat(types(processed.get(2).getOldTupleList())).containsExactly(INT4, TEXT, INT4);
    }

    @Test
    public void shouldResolveColumnTypesAgainWhenRelationMessageArrives() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", "5")));
        process(decoder, relation());
        process(decoder, insert(tuple("2", "b", "6")));

        verify(typeRegistry, times(4)).get("int4");
        verify(typeRegistry, times(2)).get("text");
        assertThat(types(processed.get(0).getNewTupleList())).containsExactly(INT4, TEXT, INT4);
        assertThat(types(processed.get(1).getNewTupleList())).containsExactly(INT4, TEXT, INT4);
    }

    @Test
    public void shouldResolveTypesOfNewColumnWhenRelationChanges() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", "5")));
        databaseColumns = new ArrayList<>(columns(false));
        databaseColumns.add(Column.editor().name("note").optional(true).create());
        process(decoder, relation(column("note", TEXT_OID)));
        process(decoder, insert(tuple("2", "b", "6", "n")));
        process(decoder, insert(tuple("3", "c", "7", "m")));

        assertThat(types(processed.get(0).getNewTupleList())).containsExactly(INT4, TEXT, INT4);
        assertThat(types(processed.get(1).getNewTupleList())).containsExactly(INT4, TEXT, INT4, TEXT);
        assertThat(types(processed.get(2).getNewTupleList())).containsExactly(INT4, TEXT, INT4, TEXT);
        assertThat(names(processed.get(2).getNewTupleList())).containsExactly("id", "val", "req", "note");
        // one lookup of "text" for the first table and two for the table with the new column
        verify(typeRegistry, times(3)).get("text");
    }

    @Test
    public void shouldAskTypeRegistryAgainWhileTypeIsUnknown() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);
        // The registry gives the placeholder for a type that the database does not know yet
        when(typeRegistry.get("text")).thenReturn(PostgresType.UNKNOWN, PostgresType.UNKNOWN, TEXT);

        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", "5")));
        process(decoder, insert(tuple("2", "b", "6")));
        process(decoder, insert(tuple("3", "c", "7")));
        process(decoder, insert(tuple("4", "d", "8")));

        assertThat(types(processed.get(0).getNewTupleList())).containsExactly(INT4, PostgresType.UNKNOWN, INT4);
        assertThat(types(processed.get(1).getNewTupleList())).containsExactly(INT4, PostgresType.UNKNOWN, INT4);
        assertThat(types(processed.get(2).getNewTupleList())).containsExactly(INT4, TEXT, INT4);
        assertThat(types(processed.get(3).getNewTupleList())).containsExactly(INT4, TEXT, INT4);
        verify(typeRegistry, times(3)).get("text");
    }

    @Test
    public void shouldCarryTableIdOfRelationInChangeMessages() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL, "none");

        process(decoder, begin());
        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", "5")));
        process(decoder, update('O', tuple("1", "a", "5"), tuple("1", "b", "5")));
        process(decoder, delete('O', tuple("1", "b", "5")));
        process(decoder, truncate());

        final List<ReplicationMessage> changes = changes();
        assertThat(changes).extracting(ReplicationMessage::getOperation).containsExactly(
                ReplicationMessage.Operation.INSERT,
                ReplicationMessage.Operation.UPDATE,
                ReplicationMessage.Operation.DELETE,
                ReplicationMessage.Operation.TRUNCATE);
        for (ReplicationMessage message : changes) {
            assertThat(message.getTable()).isEqualTo("\"public\".\"orders\"");
            assertSameTableId(message.getTableId(), PostgresSchema.parse(message.getTable()));
            assertSameTableId(message.getTableId(), TABLE_ID);
        }
    }

    @Test
    public void shouldKeepTheTableIdWhileTheTableStaysTheSame() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);

        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", "5")));
        process(decoder, insert(tuple("2", "b", "6")));

        assertThat(processed.get(0).getTableId()).isNotNull().isSameAs(processed.get(1).getTableId());
        assertThat(processed.get(0).getTable()).isEqualTo(processed.get(1).getTable());
    }

    @Test
    public void shouldCarryTableIdOfTheRenamedTable() throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);
        mockMetadataOfAnyTable();

        process(decoder, relation());
        process(decoder, insert(tuple("1", "a", "5")));
        process(decoder, relation(new TableId(null, "archive", "orders_2024")));
        process(decoder, insert(tuple("2", "b", "6")));

        assertThat(processed.get(0).getTable()).isEqualTo("\"public\".\"orders\"");
        assertSameTableId(processed.get(0).getTableId(), new TableId(null, "public", "orders"));
        assertThat(processed.get(1).getTable()).isEqualTo("\"archive\".\"orders_2024\"");
        assertSameTableId(processed.get(1).getTableId(), new TableId(null, "archive", "orders_2024"));
    }

    @Test
    public void shouldCarryTableIdThatEqualsTheParsedTableNameForNamesThatSurviveQuoting() throws Exception {
        mockMetadataOfAnyTable();

        final String[][] names = {
                { "public", "orders" },
                { "public", "Orders" },
                { "MySchema", "MyTable" },
                { "my.schema", "my.table" },
                { "public", "dotted.name.with.three.parts" },
                { "public", "with\"double\"quotes" },
                { "public", "with'single'quote" },
                { "public", "with`backtick" },
                { "public", " leading and trailing spaces " },
                { "public", "tab\tinside" },
                { "public", "UPPER_CASE_ONLY" },
                { "public", "$dollar" },
                { "public", "(parentheses)" },
                { "public", "[brackets]" },
        };
        for (String[] name : names) {
            final TableId original = new TableId(null, name[0], name[1]);

            final ReplicationMessage message = decodeInsertInto(original);

            assertThat(message.getTable()).as("table name of %s", original).isEqualTo(original.toDoubleQuotedString());
            assertThat(message.getTableId()).as("table id of %s", original).isNotNull();
            assertSameTableId(message.getTableId(), PostgresSchema.parse(message.getTable()));
            // The name survives quoting, so the table id is also the one the decoder has
            assertSameTableId(message.getTableId(), original);
        }
    }

    @Test
    public void shouldCarryTableIdThatEqualsTheParsedTableNameForNamesThatChangeWhenParsed() throws Exception {
        mockMetadataOfAnyTable();

        final String[][] names = {
                // a name that starts and ends with a double quote is not quoted again, so the quotes are lost
                { "public", "\"quoted\"" },
                // two single quotes or two backticks in a row become one
                { "public", "two''single" },
                { "public", "two``backticks" },
                // pgoutput sends an empty schema for the system catalog, and a name without schema gets the public schema
                { "", "no_schema" },
        };
        for (String[] name : names) {
            final TableId original = new TableId(null, name[0], name[1]);

            final ReplicationMessage message = decodeInsertInto(original);

            assertThat(message.getTable()).as("table name of %s", original).isEqualTo(original.toDoubleQuotedString());
            assertThat(message.getTableId()).as("table id of %s", original).isNotNull();
            assertSameTableId(message.getTableId(), PostgresSchema.parse(message.getTable()));
            // The id is what parsing the name gives and not the id of the decoder, because these two differ here
            assertThat(message.getTableId()).as("table id of %s", original).isNotEqualTo(original);
        }
    }

    @Test
    public void shouldCarryTableIdThatEqualsTheParsedTableNameForNonAsciiNames() throws Exception {
        mockMetadataOfAnyTable();

        // The decoder reads the bytes of the name as characters, and we only expect it to carry what it makes of the name
        final ReplicationMessage message = decodeInsertInto(new TableId(null, "public", "unicode_\u0442\u0430\u0431\u043b\u0438\u0446\u0430"));

        assertThat(message.getTable()).startsWith("\"public\".\"unicode_");
        assertSameTableId(message.getTableId(), PostgresSchema.parse(message.getTable()));
    }

    @Test
    public void shouldLeaveTableIdEmptyForNameThatCannotBeParsed() throws Exception {
        mockMetadataOfAnyTable();

        // A name that starts or ends with a double quote is not quoted again, and these cannot be read back
        for (String tableName : new String[]{ "\"not closed", "ends with quote\"", "\"two\" \"parts\"" }) {
            final TableId original = new TableId(null, "public", tableName);

            final ReplicationMessage message = decodeInsertInto(original);

            // The streaming source parses the name when the message has no table id, and fails the way it always did
            assertThat(message.getTableId()).as("table id of %s", original).isNull();
            assertThat(message.getTable()).isEqualTo(original.toDoubleQuotedString());
            assertThatThrownBy(() -> PostgresSchema.parse(message.getTable())).isInstanceOf(RuntimeException.class);
        }
    }

    private ReplicationMessage decodeInsertInto(TableId tableId) throws Exception {
        final PgOutputMessageDecoder decoder = decoder(TTL);
        processed.clear();
        process(decoder, relation(tableId));
        process(decoder, insert(tuple("1", "a", "5")));
        assertThat(processed).hasSize(1);
        return processed.get(0);
    }

    /**
     * The metadata queries of the mocked connection answer for any table.
     */
    private void mockMetadataOfAnyTable() throws Exception {
        when(connection.getTableColumnsForDecoder(any(), any())).thenAnswer(invocation -> databaseColumns);
        when(connection.readPrimaryKeyNames(any(), any())).thenAnswer(invocation -> new ArrayList<>(List.of("id")));
    }

    private List<ReplicationMessage> changes() {
        final List<ReplicationMessage> changes = new ArrayList<>();
        for (ReplicationMessage message : processed) {
            if (!message.isTransactionalMessage()) {
                changes.add(message);
            }
        }
        return changes;
    }

    private static void assertSameTableId(TableId actual, TableId expected) {
        assertThat(actual).isNotNull();
        assertThat(actual.catalog()).isEqualTo(expected.catalog());
        assertThat(actual.schema()).isEqualTo(expected.schema());
        assertThat(actual.table()).isEqualTo(expected.table());
        assertThat(actual).isEqualTo(expected);
    }

    private PgOutputMessageDecoder decoder(Duration ttl) {
        return decoder(ttl, null);
    }

    private PgOutputMessageDecoder decoder(Duration ttl, String skippedOperations) {
        final Configuration.Builder configuration = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.RELATION_METADATA_CACHE_TTL_MS, ttl.toMillis());
        if (skippedOperations != null) {
            configuration.with(CommonConnectorConfig.SKIPPED_OPERATIONS, skippedOperations);
        }
        final PostgresConnectorConfig config = new PostgresConnectorConfig(configuration.build());
        return new PgOutputMessageDecoder(new MessageDecoderContext(config, schema), connection, now::get);
    }

    private void process(PgOutputMessageDecoder decoder, ByteBuffer message) throws Exception {
        decoder.processMessage(message, msg -> {
            processed.add(msg);
            tablesAtProcessing.add(appliedTables.get(RELATION_ID));
        }, typeRegistry);
    }

    private void assertMetadataReads(int count) throws Exception {
        verify(connection, times(count)).getTableColumnsForDecoder(eq(TABLE_ID), any());
        verify(connection, times(count)).readPrimaryKeyNames(any(), eq(TABLE_ID));
    }

    private static List<PostgresType> types(List<ReplicationMessage.Column> columns) {
        final List<PostgresType> types = new ArrayList<>();
        for (ReplicationMessage.Column column : columns) {
            types.add(column.getType());
        }
        return types;
    }

    private static List<String> names(List<ReplicationMessage.Column> columns) {
        final List<String> names = new ArrayList<>();
        for (ReplicationMessage.Column column : columns) {
            names.add(column.getName());
        }
        return names;
    }

    private static List<Object> values(List<ReplicationMessage.Column> columns) {
        final List<Object> values = new ArrayList<>();
        for (ReplicationMessage.Column column : columns) {
            values.add(column.getValue(null, false));
        }
        return values;
    }

    private static List<Column> columns(boolean reqOptional) {
        final List<Column> columns = new ArrayList<>();
        columns.add(Column.editor().name("id").optional(false).create());
        columns.add(Column.editor().name("val").optional(true).create());
        columns.add(Column.editor().name("req").optional(reqOptional).create());
        return columns;
    }

    private static Object[] column(String name, int typeOid) {
        return new Object[]{ name, typeOid };
    }

    /**
     * Relation message for public.orders(id int4 key, val text, req int4) followed by the extra columns.
     */
    private static ByteBuffer relation(Object[]... extraColumns) throws IOException {
        return relation(TABLE_ID, extraColumns);
    }

    /**
     * Relation message for the given table with the same columns as {@link #relation(Object[]...)}.
     */
    private static ByteBuffer relation(TableId tableId, Object[]... extraColumns) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte('R');
        out.writeInt(RELATION_ID);
        writeString(out, tableId.schema());
        writeString(out, tableId.table());
        out.writeByte('d');
        out.writeShort(3 + extraColumns.length);
        writeRelationColumn(out, KEY_FLAG, "id", INT4_OID);
        writeRelationColumn(out, (byte) 0, "val", TEXT_OID);
        writeRelationColumn(out, (byte) 0, "req", INT4_OID);
        for (Object[] column : extraColumns) {
            writeRelationColumn(out, (byte) 0, (String) column[0], (Integer) column[1]);
        }
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static void writeRelationColumn(DataOutputStream out, byte flags, String name, int typeOid) throws IOException {
        out.writeByte(flags);
        writeString(out, name);
        out.writeInt(typeOid);
        out.writeInt(TypeRegistry.NO_TYPE_MODIFIER);
    }

    private static ByteBuffer begin() throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte('B');
        out.writeLong(1L); // final LSN of the transaction
        out.writeLong(0L); // commit timestamp
        out.writeInt(700); // transaction id
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static ByteBuffer truncate() throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte('T');
        out.writeInt(1); // number of relations
        out.writeByte(0); // options
        out.writeInt(RELATION_ID);
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static ByteBuffer insert(byte[] newTuple) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte('I');
        out.writeInt(RELATION_ID);
        out.writeByte('N');
        out.write(newTuple);
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static ByteBuffer update(Character oldTupleType, byte[] oldTuple, byte[] newTuple) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte('U');
        out.writeInt(RELATION_ID);
        if (oldTupleType != null) {
            out.writeByte(oldTupleType);
            out.write(oldTuple);
        }
        out.writeByte('N');
        out.write(newTuple);
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static ByteBuffer delete(char oldTupleType, byte[] oldTuple) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte('D');
        out.writeInt(RELATION_ID);
        out.writeByte(oldTupleType);
        out.write(oldTuple);
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    /**
     * Tuple data in text format; a null value is sent as 'n'.
     */
    private static byte[] tuple(String... values) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bytes);
        out.writeShort(values.length);
        for (String value : values) {
            if (value == null) {
                out.writeByte('n');
            }
            else {
                final byte[] text = value.getBytes(StandardCharsets.UTF_8);
                out.writeByte('t');
                out.writeInt(text.length);
                out.write(text);
            }
        }
        return bytes.toByteArray();
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        out.write(value.getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
    }
}
