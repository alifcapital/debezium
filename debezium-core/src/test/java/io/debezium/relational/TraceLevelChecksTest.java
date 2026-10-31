/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Loggings;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;

/**
 * Checks how often the per-value code paths ask the logger for its level, and that they log the same
 * messages at TRACE level.
 */
public class TraceLevelChecksTest {

    private static final int COLUMN_COUNT = 10;

    private final List<Logger> changedLoggers = new ArrayList<>();
    private final Map<Logger, Level> originalLevels = new HashMap<>();
    private final Map<String, Integer> traceDecisions = new HashMap<>();
    private final Map<ListAppender<ILoggingEvent>, Logger> appenders = new HashMap<>();
    private final TurboFilter countingFilter = new TurboFilter() {
        @Override
        public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] params, Throwable t) {
            if (level == Level.TRACE) {
                traceDecisions.merge(logger.getName(), 1, Integer::sum);
            }
            return FilterReply.NEUTRAL;
        }
    };

    private LoggerContext loggerContext;

    @Before
    public void beforeEach() {
        loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.addTurboFilter(countingFilter);
    }

    @After
    public void afterEach() {
        loggerContext.getTurboFilterList().remove(countingFilter);
        appenders.forEach((appender, logger) -> {
            logger.detachAppender(appender);
            appender.stop();
        });
        for (Logger logger : changedLoggers) {
            logger.setLevel(originalLevels.get(logger));
        }
    }

    @Test
    public void shouldCheckTraceLevelOnceForConvertedValue() {
        setLevel(JdbcValueConverters.class, Level.INFO);
        setLevel(Loggings.class, Level.INFO);
        final ValueConverter converter = integerConverter(new JdbcValueConverters());
        traceDecisions.clear();

        assertThat(converter.convert(5)).isEqualTo(5);

        assertThat(traceDecisions.getOrDefault(JdbcValueConverters.class.getName(), 0)).isEqualTo(1);
        assertThat(traceDecisions.getOrDefault(Loggings.class.getName(), 0)).isZero();
    }

    @Test
    public void shouldNotCheckTraceLevelForNullValue() {
        setLevel(JdbcValueConverters.class, Level.INFO);
        setLevel(Loggings.class, Level.INFO);
        final ValueConverter converter = integerConverter(new JdbcValueConverters());
        traceDecisions.clear();

        assertThat(converter.convert(null)).isNull();

        assertThat(traceDecisions.getOrDefault(JdbcValueConverters.class.getName(), 0)).isZero();
        assertThat(traceDecisions.getOrDefault(Loggings.class.getName(), 0)).isZero();
    }

    @Test
    public void shouldLogConvertedValueAtTraceLevel() {
        setLevel(JdbcValueConverters.class, Level.TRACE);
        setLevel(Loggings.class, Level.TRACE);
        final List<ILoggingEvent> events = capture(JdbcValueConverters.class);
        final ValueConverter converter = integerConverter(new JdbcValueConverters());

        assertThat(converter.convert(5)).isEqualTo(5);

        final List<String> messages = events.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
        assertThat(messages).hasSize(3);
        assertThat(messages.get(0)).isEqualTo("Value from data object: *** 5 ***");
        assertThat(messages.get(1)).startsWith("Callback is: ");
        assertThat(messages.get(2)).startsWith("Value from ResultReceiver: ").doesNotContain("[REDACTED]");
    }

    @Test
    public void shouldRedactConvertedValueWhenOnlyTheConverterLoggerIsAtTraceLevel() {
        setLevel(JdbcValueConverters.class, Level.TRACE);
        setLevel(Loggings.class, Level.INFO);
        final List<ILoggingEvent> events = capture(JdbcValueConverters.class);
        final ValueConverter converter = integerConverter(new JdbcValueConverters());

        assertThat(converter.convert(5)).isEqualTo(5);

        final List<String> messages = events.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
        assertThat(messages).hasSize(3);
        assertThat(messages.get(0)).isEqualTo("Value from data object: *** [REDACTED] ***");
        assertThat(messages.get(1)).startsWith("Callback is: ");
        assertThat(messages.get(2)).isEqualTo("Value from ResultReceiver: [REDACTED]");
    }

    @Test
    public void shouldNotLogConvertedValueBelowTraceLevel() {
        setLevel(JdbcValueConverters.class, Level.DEBUG);
        setLevel(Loggings.class, Level.TRACE);
        final List<ILoggingEvent> events = capture(JdbcValueConverters.class);
        final ValueConverter converter = integerConverter(new JdbcValueConverters());

        assertThat(converter.convert(5)).isEqualTo(5);

        assertThat(events).isEmpty();
    }

    @Test
    public void shouldCheckTraceLevelOncePerRowWhenGeneratingValue() {
        setLevel(TableSchemaBuilder.class, Level.INFO);
        final TableSchema tableSchema = tableSchema();
        final Object[] row = row();
        traceDecisions.clear();

        final Struct value = tableSchema.valueFromColumnData(row);

        assertThat(value.get("c0")).isEqualTo(0);
        assertThat(value.get("c" + (COLUMN_COUNT - 1))).isEqualTo(COLUMN_COUNT - 1);
        assertThat(traceDecisions.getOrDefault(TableSchemaBuilder.class.getName(), 0)).isEqualTo(1);
    }

    @Test
    public void shouldLogConverterOfEveryColumnAtTraceLevel() {
        setLevel(TableSchemaBuilder.class, Level.TRACE);
        final List<ILoggingEvent> events = capture(TableSchemaBuilder.class);
        final TableSchema tableSchema = tableSchema();
        final Object[] row = row();
        events.clear();

        tableSchema.valueFromColumnData(row);

        final List<String> messages = events.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
        assertThat(messages).hasSize(COLUMN_COUNT);
        assertThat(messages).allMatch(message -> message.startsWith("converter for value object: *** ") && message.endsWith(" ***"));
    }

    private void setLevel(Class<?> loggerClass, Level level) {
        final Logger logger = (Logger) LoggerFactory.getLogger(loggerClass);
        if (!changedLoggers.contains(logger)) {
            changedLoggers.add(logger);
            originalLevels.put(logger, logger.getLevel());
        }
        logger.setLevel(level);
    }

    private List<ILoggingEvent> capture(Class<?> loggerClass) {
        final Logger logger = (Logger) LoggerFactory.getLogger(loggerClass);
        final ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.setContext(loggerContext);
        appender.start();
        logger.addAppender(appender);
        appenders.put(appender, logger);
        return appender.list;
    }

    private static ValueConverter integerConverter(JdbcValueConverters converters) {
        final Column column = Column.editor().name("c").type("INT").jdbcType(Types.INTEGER).optional(true).create();
        final Field field = new Field(column.name(), -1, converters.schemaBuilder(column).build());
        return converters.converter(column, field);
    }

    private static TableSchema tableSchema() {
        final TableEditor editor = Table.editor().tableId(new TableId(null, "public", "wide"));
        for (int i = 0; i < COLUMN_COUNT; i++) {
            editor.addColumn(Column.editor().name("c" + i).type("INT").jdbcType(Types.INTEGER).optional(true).create());
        }
        final Table table = editor.setPrimaryKeyNames("c0").create();
        final Properties topicProperties = new Properties();
        topicProperties.put("topic.prefix", "test");
        final TopicNamingStrategy topicNamingStrategy = new DefaultTopicNamingStrategy(topicProperties);
        return new TableSchemaBuilder(new JdbcValueConverters(), null, SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null),
                SchemaBuilder.struct().build(), FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false)
                .create(topicNamingStrategy, table, null, null, null);
    }

    private static Object[] row() {
        final Object[] row = new Object[COLUMN_COUNT];
        for (int i = 0; i < COLUMN_COUNT; i++) {
            row[i] = i;
        }
        return row;
    }
}
