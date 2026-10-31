/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Checks how the change record emitter places the values of a replication message into the row of the table.
 */
public class PostgresChangeRecordEmitterTest {

    private static final TableId TABLE_ID = new TableId(null, "public", "orders");

    private PostgresSchema schema;
    private PostgresConnectorConfig connectorConfig;

    @Before
    public void beforeEach() {
        schema = mock(PostgresSchema.class);
        connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
    }

    @Test
    public void shouldPlaceValuesOfColumnsInTableOrder() {
        final Table table = table("id", "name", "Price");

        final Object[] values = newColumnValues(table, message(column("id", 1), column("name", "a"), column("Price", 5)));

        assertThat(values).containsExactly(1, "a", 5);
    }

    @Test
    public void shouldPlaceValuesOfColumnsInTheirOwnPositionWhenMessageOrderDiffers() {
        final Table table = table("id", "name", "price");

        final Object[] values = newColumnValues(table, message(column("price", 5), column("id", 1), column("name", "a")));

        assertThat(values).containsExactly(1, "a", 5);
    }

    @Test
    public void shouldMatchColumnNameIgnoringCaseWhenNamesAtTheSameIndexDiffer() {
        final Table table = table("id", "Name", "price");

        final Object[] values = newColumnValues(table, message(column("ID", 1), column("name", "a"), column("PRICE", 5)));

        assertThat(values).containsExactly(1, "a", 5);
    }

    @Test
    public void shouldPlaceValuesOfMessageWithFewerColumns() {
        final Table table = table("id", "name", "price");

        final Object[] values = newColumnValues(table, message(column("id", 1), column("name", "a")));

        assertThat(values).containsExactly(1, "a", null);
    }

    @Test
    public void shouldPlaceValuesOfMessageWithMoreColumnsThanTheTable() {
        final Table table = table("id", "name");

        final LogInterceptor logInterceptor = new LogInterceptor(PostgresChangeRecordEmitter.class);
        final Object[] values = newColumnValues(table, message(column("id", 1), column("name", "a"), column("extra", 5)));

        // The extra column is not part of the table, so it is omitted and the row has a slot for every message column
        assertThat(values).containsExactly(1, "a", null);
        assertThat(logInterceptor.containsWarnMessage("column extra will be omitted from the change event")).isTrue();
    }

    @Test
    public void shouldOmitUnknownColumnAtTheIndexOfAKnownColumnAndKeepTheOthers() {
        final Table table = table("id", "name", "price");

        final LogInterceptor logInterceptor = new LogInterceptor(PostgresChangeRecordEmitter.class);
        final Object[] values = newColumnValues(table, message(column("id", 1), column("renamed", "a"), column("price", 5)));

        assertThat(values).containsExactly(1, null, 5);
        assertThat(logInterceptor.containsWarnMessage("column renamed will be omitted from the change event")).isTrue();
    }

    @Test
    public void shouldNotLookUpColumnsByNameWhenMessageFollowsTableOrder() {
        final Table table = mock(Table.class);
        final Table realTable = table("id", "name", "price");
        when(table.id()).thenReturn(TABLE_ID);
        when(table.columns()).thenReturn(realTable.columns());
        when(schema.tableFor(TABLE_ID)).thenReturn(table);

        final Object[] values = newColumnValues(table, message(column("id", 1), column("name", "a"), column("price", 5)));

        assertThat(values).containsExactly(1, "a", 5);
        verify(table, never()).columnWithName(any());
    }

    @Test
    public void shouldLookUpOnlyTheColumnsOutOfOrderByName() {
        final Table table = mock(Table.class);
        final Table realTable = table("id", "name", "price");
        when(table.id()).thenReturn(TABLE_ID);
        when(table.columns()).thenReturn(realTable.columns());
        when(table.columnWithName(any())).thenAnswer(invocation -> realTable.columnWithName(invocation.getArgument(0)));
        when(schema.tableFor(TABLE_ID)).thenReturn(table);

        // "id" is in place, "price" and "name" are swapped
        final Object[] values = newColumnValues(table, message(column("id", 1), column("price", 5), column("name", "a")));

        assertThat(values).containsExactly(1, "a", 5);
        verify(table, times(2)).columnWithName(any());
        verify(table, times(1)).columnWithName("price");
        verify(table, times(1)).columnWithName("name");
    }

    private Object[] newColumnValues(Table table, ReplicationMessage message) {
        when(schema.tableFor(TABLE_ID)).thenReturn(table);
        final PostgresChangeRecordEmitter emitter = new PostgresChangeRecordEmitter(mock(PostgresPartition.class), mock(OffsetContext.class), Clock.system(),
                connectorConfig, schema, mock(PostgresConnection.class), TABLE_ID, message);
        return emitter.getNewColumnValues();
    }

    private static Table table(String... columnNames) {
        final List<Column> columns = new ArrayList<>();
        for (String columnName : columnNames) {
            columns.add(Column.editor().name(columnName).type("text").jdbcType(Types.VARCHAR).optional(true).create());
        }
        return Table.editor().tableId(TABLE_ID).addColumns(columns).create();
    }

    private static ReplicationMessage message(ReplicationMessage.Column... columns) {
        final ReplicationMessage message = mock(ReplicationMessage.class);
        when(message.getOperation()).thenReturn(ReplicationMessage.Operation.INSERT);
        when(message.getNewTupleList()).thenReturn(List.of(columns));
        return message;
    }

    private static ReplicationMessage.Column column(String name, Object value) {
        final ReplicationMessage.Column column = mock(ReplicationMessage.Column.class);
        when(column.getName()).thenReturn(name);
        when(column.getValue(any(), org.mockito.ArgumentMatchers.anyBoolean())).thenReturn(value);
        return column;
    }
}
