/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresValueConverter;

/**
 * Transformer for time/date related string representations in replication messages.
 *
 * @author Jiri Pechanec
 *
 */
public interface DateTimeFormat {
    Instant timestampToInstant(String s);

    OffsetDateTime timestampWithTimeZoneToOffsetDateTime(String s);

    Instant systemTimestampToInstant(String s);

    LocalDate date(String s);

    LocalTime time(String s);

    OffsetTime timeWithTimeZone(String s);

    static DateTimeFormat get() {
        return new ISODateTimeFormat();
    }

    class ISODateTimeFormat implements DateTimeFormat {
        private static final Logger LOGGER = LoggerFactory.getLogger(ISODateTimeFormat.class);

        private static final int DATE_LENGTH = "YYYY-MM-DD".length();
        private static final int TIMESTAMP_LENGTH = "YYYY-MM-DD HH:MM:SS".length();
        private static final int MAX_FRACTION_DIGITS = 9;
        private static final long SECONDS_PER_DAY = 86_400L;

        // This formatter is similar to standard Java's ISO_LOCAL_DATE. But this one is
        // using 'YEAR_OF_ERA + SignStyle.NEVER' instead of 'YEAR+SignStyle.EXCEEDS_PAD'
        // to support ChronoField.ERA at the end of the date string.
        private static final DateTimeFormatter NON_ISO_LOCAL_DATE = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR_OF_ERA, 4, 10, SignStyle.NEVER)
                .appendLiteral('-')
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendLiteral('-')
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .toFormatter();

        private static final String TS_FORMAT_PATTERN_HINT = "y..y-MM-dd HH:mm:ss[.S]";
        private static final DateTimeFormatter TS_FORMAT = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .optionalStart()
                .appendLiteral(" ")
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();

        private static final String TS_TZ_FORMAT_PATTERN_HINT = "y..y-MM-dd HH:mm:ss[.S]X";
        private static final DateTimeFormatter TS_TZ_FORMAT = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .appendOffset("+HH:mm", "")
                .optionalStart()
                .appendLiteral(" ")
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();
        private static final DateTimeFormatter TS_TZ_WITH_SECONDS_FORMAT = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .appendOffset("+HH:MM:SS", "")
                .optionalStart()
                .appendLiteral(" ")
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();

        private static final String SYSTEM_TS_FORMAT_PATTERN_HINT = "y..y-MM-dd HH:mm:ss.SSSSSSX";
        private static final DateTimeFormatter SYSTEM_TS_FORMAT = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .appendOffset("+HH:mm", "Z")
                .optionalStart()
                .appendLiteral(" ")
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();

        private static final String DATE_FORMAT_OPT_ERA_PATTERN_HINT = "y..y-MM-dd[ GG]";
        private static final DateTimeFormatter DATE_FORMAT_OPT_ERA = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .optionalStart()
                .appendLiteral(' ')
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();

        private static final String TIME_FORMAT_PATTERN = "HH:mm:ss[.S]";
        private static final DateTimeFormatter TIME_FORMAT = new DateTimeFormatterBuilder()
                .appendPattern("HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .toFormatter();

        private static final String TIME_TZ_FORMAT_PATTERN = "HH:mm:ss[.S]X";
        private static final DateTimeFormatter TIME_TZ_FORMAT = new DateTimeFormatterBuilder()
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .appendOffset("+HH:mm", "")
                .toFormatter();

        @Override
        public LocalDate date(final String s) {
            final LocalDate fastValue = parseIsoDate(s);
            return fastValue != null ? fastValue : dateWithFormatter(s);
        }

        LocalDate dateWithFormatter(final String s) {
            return format(DATE_FORMAT_OPT_ERA_PATTERN_HINT, s, () -> {
                if ("infinity".equals(s)) {
                    return PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE;
                }
                else if ("-infinity".equals(s)) {
                    return PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE;
                }
                else {
                    return LocalDate.parse(s, DATE_FORMAT_OPT_ERA);
                }
            });
        }

        @Override
        public LocalTime time(final String s) {
            return format(TIME_FORMAT_PATTERN, s, () -> LocalTime.parse(s, TIME_FORMAT));
        }

        @Override
        public OffsetTime timeWithTimeZone(final String s) {
            return format(TIME_TZ_FORMAT_PATTERN, s, () -> OffsetTime.parse(s, TIME_TZ_FORMAT)).withOffsetSameInstant(ZoneOffset.UTC);
        }

        private <T> T format(final String pattern, final String s, final Supplier<T> value) {
            try {
                return value.get();
            }
            catch (final DateTimeParseException e) {
                LOGGER.error("Cannot parse time/date value '{}', expected format '{}'", s, pattern);
                throw new ConnectException(e);
            }
        }

        @Override
        public Instant timestampToInstant(String s) {
            final Instant fastValue = parseIsoTimestamp(s);
            return fastValue != null ? fastValue : timestampToInstantWithFormatter(s);
        }

        Instant timestampToInstantWithFormatter(String s) {
            return format(TS_FORMAT_PATTERN_HINT, s, () -> LocalDateTime.from(TS_FORMAT.parse(s)).toInstant(ZoneOffset.UTC));
        }

        @Override
        public OffsetDateTime timestampWithTimeZoneToOffsetDateTime(String s) {
            return format(TS_TZ_FORMAT_PATTERN_HINT, s, () -> {
                TemporalAccessor parsedTimestamp;
                // Usually the timestamp contains only hour offset and optionally minutes
                // For very large negative timestamps the offset could contain seconds
                // The standard parsing library does not allow both optional minutes and seconds in offset,
                // so it is necessary to parse it with optional minutes and if that fails then retyr with
                // seconds
                try {
                    parsedTimestamp = TS_TZ_FORMAT.parse(s);
                }
                catch (DateTimeParseException e) {
                    parsedTimestamp = TS_TZ_WITH_SECONDS_FORMAT.parse(s);
                }
                return OffsetDateTime.from(parsedTimestamp);
            });
        }

        @Override
        public Instant systemTimestampToInstant(String s) {
            return format(SYSTEM_TS_FORMAT_PATTERN_HINT, s, () -> OffsetDateTime.from(SYSTEM_TS_FORMAT.parse(s)).toInstant());
        }

        // Parsing each value with the formatter is slow and it runs for every date and timestamp column.
        // We fear that the formatter also accepts or fixes up inputs in its own way (for example it moves
        // 2023-02-30 to 2023-02-28), so we only take the plain valid form here and return null for anything
        // else. The caller then uses the formatter, which gives the same value or the same exception.
        static LocalDate parseIsoDate(final String s) {
            if (s == null || s.length() != DATE_LENGTH) {
                return null;
            }
            return parsePlainDate(s);
        }

        // Accepts YYYY-MM-DD HH:MM:SS with an optional dot and 1 to 9 fraction digits, read as UTC.
        static Instant parseIsoTimestamp(final String s) {
            if (s == null) {
                return null;
            }
            final int length = s.length();
            // A dot must be followed by at least one digit, so the length 20 is not possible
            if (length < TIMESTAMP_LENGTH || length == TIMESTAMP_LENGTH + 1 || length > TIMESTAMP_LENGTH + 1 + MAX_FRACTION_DIGITS) {
                return null;
            }
            if (s.charAt(10) != ' ' || s.charAt(13) != ':' || s.charAt(16) != ':') {
                return null;
            }

            final LocalDate date = parsePlainDate(s);
            final int hour = twoDigits(s, 11);
            final int minute = twoDigits(s, 14);
            final int second = twoDigits(s, 17);
            // The formatter also accepts 24:00:00 as the end of the day, we leave that to it
            if (date == null || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
                return null;
            }

            int nanos = 0;
            if (length > TIMESTAMP_LENGTH) {
                if (s.charAt(TIMESTAMP_LENGTH) != '.') {
                    return null;
                }
                int digits = 0;
                for (int i = TIMESTAMP_LENGTH + 1; i < length; i++) {
                    final int digit = s.charAt(i) - '0';
                    if (digit < 0 || digit > 9) {
                        return null;
                    }
                    nanos = nanos * 10 + digit;
                    digits++;
                }
                for (; digits < MAX_FRACTION_DIGITS; digits++) {
                    nanos *= 10;
                }
            }

            final long epochSecond = date.toEpochDay() * SECONDS_PER_DAY + hour * 3600 + minute * 60 + second;
            return Instant.ofEpochSecond(epochSecond, nanos);
        }

        // Reads the YYYY-MM-DD at the start of the string, which must be at least 10 characters long.
        private static LocalDate parsePlainDate(final String s) {
            if (s.charAt(4) != '-' || s.charAt(7) != '-') {
                return null;
            }
            final int yearHigh = twoDigits(s, 0);
            final int yearLow = twoDigits(s, 2);
            final int month = twoDigits(s, 5);
            final int day = twoDigits(s, 8);
            if (yearHigh < 0 || yearLow < 0 || month < 1 || month > 12 || day < 1) {
                return null;
            }
            final int year = yearHigh * 100 + yearLow;
            if (year < 1 || day > Month.of(month).length(Year.isLeap(year))) {
                return null;
            }
            return LocalDate.of(year, month, day);
        }

        // Returns -1 if one of the two characters is not an ASCII digit.
        private static int twoDigits(final String s, final int position) {
            final int tens = s.charAt(position) - '0';
            final int ones = s.charAt(position + 1) - '0';
            if (tens < 0 || tens > 9 || ones < 0 || ones > 9) {
                return -1;
            }
            return tens * 10 + ones;
        }
    }
}
