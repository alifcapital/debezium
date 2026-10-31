/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.chrono.IsoEra;
import java.time.format.TextStyle;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Test;

import io.debezium.connector.postgresql.connection.DateTimeFormat.ISODateTimeFormat;

/**
 * Checks that the direct parsing of the usual date and timestamp strings gives exactly what the formatter gives,
 * and that everything else is left to the formatter.
 */
public class ISODateTimeFormatFastPathTest {

    private static final String BCE_DISPLAY_NAME = IsoEra.BCE.getDisplayName(TextStyle.SHORT, Locale.getDefault());

    private final ISODateTimeFormat format = new ISODateTimeFormat();

    @Test
    public void shouldParseEveryDayOfEveryYearLikeFormatter() {
        IntStream.rangeClosed(1, 9999).parallel().forEach(year -> {
            final Random random = new Random(year);
            final boolean leap = Year.isLeap(year);
            for (int month = 1; month <= 12; month++) {
                final int days = Month.of(month).length(leap);
                for (int day = 1; day <= days; day++) {
                    final String date = String.format("%04d-%02d-%02d", year, month, day);
                    assertThat(ISODateTimeFormat.parseIsoDate(date)).as(date).isEqualTo(LocalDate.of(year, month, day));
                    assertSameOutcome(date, () -> format.date(date), () -> format.dateWithFormatter(date));

                    // the time of day and the fraction change with the day, together they cover the whole range
                    final String timestamp = date + " " + randomTime(random);
                    assertThat(ISODateTimeFormat.parseIsoTimestamp(timestamp)).as(timestamp).isNotNull();
                    assertSameOutcome(timestamp, () -> format.timestampToInstant(timestamp), () -> format.timestampToInstantWithFormatter(timestamp));
                }
            }
        });
    }

    @Test
    public void shouldParseEveryTimeOfDayLikeFormatter() {
        for (final String date : Arrays.asList("0001-01-01", "1969-12-31", "1970-01-01", "2000-02-29", "2016-11-04", "9999-12-31")) {
            for (int hour = 0; hour <= 23; hour++) {
                for (int minute = 0; minute <= 59; minute++) {
                    for (int second = 0; second <= 59; second++) {
                        final String timestamp = String.format("%s %02d:%02d:%02d", date, hour, minute, second);
                        assertThat(ISODateTimeFormat.parseIsoTimestamp(timestamp)).as(timestamp).isNotNull();
                        assertSameOutcome(timestamp, () -> format.timestampToInstant(timestamp), () -> format.timestampToInstantWithFormatter(timestamp));
                    }
                }
            }
        }
    }

    @Test
    public void shouldParseEveryFractionLengthLikeFormatter() {
        final Random random = new Random(7);
        for (int digits = 1; digits <= 9; digits++) {
            for (int i = 0; i < 20_000; i++) {
                final StringBuilder fraction = new StringBuilder();
                for (int d = 0; d < digits; d++) {
                    fraction.append((char) ('0' + random.nextInt(10)));
                }
                assertFractionLikeFormatter(fraction.toString());

                // zeros at the end, with and without the digits Postgres would trim
                final String trimmed = fraction.toString().replaceAll("0+$", "");
                if (!trimmed.isEmpty()) {
                    assertFractionLikeFormatter(trimmed);
                }
            }
            assertFractionLikeFormatter("0".repeat(digits));
            assertFractionLikeFormatter("9".repeat(digits));
            assertFractionLikeFormatter("1" + "0".repeat(digits - 1));
        }
    }

    @Test
    public void shouldParseRealisticValues() {
        assertThat(ISODateTimeFormat.parseIsoTimestamp("2016-11-04 13:51:30")).isEqualTo(Instant.parse("2016-11-04T13:51:30Z"));
        assertThat(ISODateTimeFormat.parseIsoTimestamp("2016-11-04 13:51:30.5")).isEqualTo(Instant.parse("2016-11-04T13:51:30.5Z"));
        assertThat(ISODateTimeFormat.parseIsoTimestamp("2016-11-04 13:51:30.123456")).isEqualTo(Instant.parse("2016-11-04T13:51:30.123456Z"));
        assertThat(ISODateTimeFormat.parseIsoTimestamp("2016-11-04 13:51:30.123456789")).isEqualTo(Instant.parse("2016-11-04T13:51:30.123456789Z"));
        assertThat(ISODateTimeFormat.parseIsoTimestamp("0001-01-01 00:00:00")).isEqualTo(Instant.parse("0001-01-01T00:00:00Z"));
        assertThat(ISODateTimeFormat.parseIsoTimestamp("9999-12-31 23:59:59.999999")).isEqualTo(Instant.parse("9999-12-31T23:59:59.999999Z"));
        assertThat(ISODateTimeFormat.parseIsoTimestamp("1970-01-01 00:00:00")).isEqualTo(Instant.EPOCH);
        assertThat(ISODateTimeFormat.parseIsoTimestamp("1969-12-31 23:59:59.999999")).isEqualTo(Instant.EPOCH.minusNanos(1000));
        assertThat(ISODateTimeFormat.parseIsoDate("2024-02-29")).isEqualTo(LocalDate.of(2024, 2, 29));
        assertThat(ISODateTimeFormat.parseIsoDate("0001-01-01")).isEqualTo(LocalDate.of(1, 1, 1));
        assertThat(ISODateTimeFormat.parseIsoDate("9999-12-31")).isEqualTo(LocalDate.of(9999, 12, 31));
    }

    @Test
    public void shouldLeaveOtherTimestampsToFormatter() {
        final List<String> inputs = Arrays.asList(
                "0001-01-01 00:00:00 BC",
                "0001-01-01 00:00:00 " + BCE_DISPLAY_NAME,
                "0002-12-01 17:00:00 " + BCE_DISPLAY_NAME,
                "2016-11-04 13:51:30 AD",
                "2016-11-04 13:51:30.123456 " + BCE_DISPLAY_NAME,
                "10000-01-01 00:00:00",
                "20160-11-04 13:51:30.123456",
                "12345678-11-04 13:51:30",
                "0000-01-01 00:00:00",
                "0000-12-31 23:59:59",
                "infinity",
                "-infinity",
                "2024-02-30 00:00:00",
                "2024-02-31 00:00:00",
                "2023-02-29 00:00:00",
                "1900-02-29 00:00:00",
                "2024-04-31 00:00:00",
                "2024-06-31 00:00:00",
                "2024-09-31 00:00:00",
                "2024-11-31 00:00:00",
                "2024-13-01 00:00:00",
                "2024-00-10 00:00:00",
                "2024-01-00 00:00:00",
                "2024-01-32 00:00:00",
                "2024-1-01 00:00:00",
                "2024-01-1 00:00:00",
                "24-01-01 00:00:00",
                "2024-01-01 0:00:00",
                "2024-01-01 00:0:00",
                "2024-01-01 00:00:0",
                "2024-01-01  00:00:00",
                " 2024-01-01 00:00:00",
                "2024-01-01 00:00:00 ",
                "2024-01-01\t00:00:00",
                "2024-01-01T00:00:00",
                "2024/01/01 00:00:00",
                "2024-01-01 00-00-00",
                "2024-01-01 24:00:00",
                "2024-01-01 24:00:01",
                "2024-01-01 24:00:00.000001",
                "2024-01-01 25:00:00",
                "2024-01-01 23:60:00",
                "2024-01-01 23:59:60",
                "2024-01-01 12:30",
                "2024-01-01 12",
                "2024-01-01 12:30:15.",
                "2024-01-01 12:30:15.1234567890",
                "2024-01-01 12:30:15.12345678901234",
                "2024-01-01 12:30:15,123",
                "2024-01-01 12:30:15:123",
                "2024-01-01 12:30:15.12a",
                "2024-01-01 12:30:15.a",
                "2024-01-01 12:30:15 .123",
                "2024-01-01 12:30:15+02",
                "2024-01-01 12:30:15.123+02",
                "2024-01-01 12:30:15.123+02:30",
                "2024-01-01 12:30:15Z",
                "2024-01-01 12:30:15.123Z",
                "abcd-ef-gh ij:kl:mn",
                "2024-01-01 ab:cd:ef",
                "202a-01-01 00:00:00",
                "2024-0a-01 00:00:00",
                "2024-01-0a 00:00:00",
                "-2024-01-01 00:00:00",
                "+2024-01-01 00:00:00",
                "-024-01-01 00:00:00",
                "2024-01-01 -1:00:00",
                "2024-01-01 +1:00:00",
                "2024-01-01 00:-1:00",
                "2024-01-01 00:00:-1",
                "2024-01-01 00:00:00.-1",
                "٢٠٢٤-01-01 00:00:00",
                "2024-01-01 ٠٠:00:00",
                "2024-01-01 00:00:00.١٢",
                "2024-01-01 00:00:00\u0000",
                "",
                " ",
                "2024",
                "2024-01-01",
                "x");
        for (final String input : inputs) {
            assertThat(ISODateTimeFormat.parseIsoTimestamp(input)).as(input).isNull();
            assertSameOutcome(input, () -> format.timestampToInstant(input), () -> format.timestampToInstantWithFormatter(input));
        }
    }

    @Test
    public void shouldLeaveOtherDatesToFormatter() {
        final List<String> inputs = Arrays.asList(
                "2016-11-04 " + BCE_DISPLAY_NAME,
                "0001-01-01 BC",
                "2016-11-04 AD",
                "20160-11-04",
                "20160-11-04 " + BCE_DISPLAY_NAME,
                "12345678-11-04",
                "10000-01-01",
                "0000-01-01",
                "0000-12-31",
                "infinity",
                "-infinity",
                "2024-02-30",
                "2024-02-31",
                "2023-02-29",
                "1900-02-29",
                "2024-04-31",
                "2024-06-31",
                "2024-09-31",
                "2024-11-31",
                "2024-13-01",
                "2024-00-10",
                "2024-01-00",
                "2024-01-32",
                "2024-1-01",
                "2024-01-1",
                "24-01-01",
                "2024-1-1",
                "2024/01/01",
                "2024-01/01",
                "2024.01.01",
                " 2024-01-01",
                "2024-01-01 ",
                "2024-01-01\t",
                "2024-01-01T00:00:00",
                "2024-01-01 00:00:00",
                "abcd-ef-gh",
                "202a-01-01",
                "2024-0a-01",
                "2024-01-0a",
                "-024-01-01",
                "+024-01-01",
                "2024--1-01",
                "2024-01--1",
                "٢٠٢٤-01-01",
                "2024-٠١-01",
                "2024-01-01\u0000",
                "",
                " ",
                "2024",
                "x");
        for (final String input : inputs) {
            assertThat(ISODateTimeFormat.parseIsoDate(input)).as(input).isNull();
            assertSameOutcome(input, () -> format.date(input), () -> format.dateWithFormatter(input));
        }
    }

    @Test
    public void shouldKeepFormatterBehaviourForNull() {
        assertThat(ISODateTimeFormat.parseIsoTimestamp(null)).isNull();
        assertThat(ISODateTimeFormat.parseIsoDate(null)).isNull();
        assertSameOutcome("null", () -> format.timestampToInstant(null), () -> format.timestampToInstantWithFormatter(null));
        assertSameOutcome("null", () -> format.date(null), () -> format.dateWithFormatter(null));
    }

    @Test
    public void shouldMatchFormatterForMutatedValues() {
        final Random random = new Random(2024);
        final String alphabet = " -:.0123456789abTZ+/,١";
        final String[] seeds = {
                "2016-11-04 13:51:30",
                "2016-11-04 13:51:30.1",
                "2016-11-04 13:51:30.123456",
                "2016-11-04 13:51:30.123456789",
                "2024-02-29 23:59:59.999999",
                "0001-01-01 00:00:00",
                "9999-12-31 23:59:59.999999",
                "2016-11-04",
                "0001-01-01",
                "9999-12-31"
        };
        for (int i = 0; i < 50_000; i++) {
            final StringBuilder sb = new StringBuilder(seeds[random.nextInt(seeds.length)]);
            final int mutations = 1 + random.nextInt(3);
            for (int m = 0; m < mutations && sb.length() > 0; m++) {
                final int position = random.nextInt(sb.length());
                switch (random.nextInt(4)) {
                    case 0:
                        sb.setCharAt(position, alphabet.charAt(random.nextInt(alphabet.length())));
                        break;
                    case 1:
                        sb.insert(position, alphabet.charAt(random.nextInt(alphabet.length())));
                        break;
                    case 2:
                        sb.deleteCharAt(position);
                        break;
                    default:
                        sb.setLength(position);
                        break;
                }
            }
            final String input = sb.toString();
            assertSameOutcome(input, () -> format.timestampToInstant(input), () -> format.timestampToInstantWithFormatter(input));
            assertSameOutcome(input, () -> format.date(input), () -> format.dateWithFormatter(input));
        }
    }

    private void assertFractionLikeFormatter(final String fraction) {
        final String timestamp = "2016-11-04 13:51:30." + fraction;
        assertThat(ISODateTimeFormat.parseIsoTimestamp(timestamp)).as(timestamp).isNotNull();
        assertSameOutcome(timestamp, () -> format.timestampToInstant(timestamp), () -> format.timestampToInstantWithFormatter(timestamp));
    }

    private static String randomTime(final Random random) {
        final StringBuilder sb = new StringBuilder(String.format("%02d:%02d:%02d", random.nextInt(24), random.nextInt(60), random.nextInt(60)));
        final int digits = random.nextInt(10);
        if (digits > 0) {
            sb.append('.');
            for (int i = 0; i < digits; i++) {
                sb.append((char) ('0' + random.nextInt(10)));
            }
        }
        return sb.toString();
    }

    /**
     * Both suppliers must either return equal values or throw the same exception type, message and cause.
     */
    private static void assertSameOutcome(final String input, final Supplier<?> actual, final Supplier<?> expected) {
        final Outcome expectedOutcome = Outcome.of(expected);
        final Outcome actualOutcome = Outcome.of(actual);
        assertThat(actualOutcome.toString()).as("input '%s'", input).isEqualTo(expectedOutcome.toString());
        assertThat(actualOutcome.value).as("input '%s'", input).isEqualTo(expectedOutcome.value);
    }

    private static final class Outcome {
        private final Object value;
        private final Throwable error;

        private Outcome(final Object value, final Throwable error) {
            this.value = value;
            this.error = error;
        }

        static Outcome of(final Supplier<?> supplier) {
            try {
                return new Outcome(supplier.get(), null);
            }
            catch (RuntimeException e) {
                return new Outcome(null, e);
            }
        }

        @Override
        public String toString() {
            if (error == null) {
                return "value " + value;
            }
            final Throwable cause = error.getCause();
            return "error " + error.getClass().getName() + ": " + error.getMessage()
                    + (cause == null ? "" : " / cause " + cause.getClass().getName() + ": " + cause.getMessage());
        }
    }
}
