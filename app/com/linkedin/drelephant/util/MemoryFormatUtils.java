package com.linkedin.drelephant.util;

import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;


/**
 * This is a utils class that handles memory string parsing and formatting problem.
 */
public class MemoryFormatUtils {
  private MemoryFormatUtils() {
    // Do nothing, empty on purpose
  }

  private static final long B = 1L;
  private static final long K = 1L << 10;
  private static final long M = 1L << 20;
  private static final long G = 1L << 30;
  private static final long T = 1L << 40;
  private static final String[] UNITS = new String[]{"TB", "GB", "MB", "KB", "B"};
  private static final long[] DIVIDERS = new long[]{T, G, M, K, B};
  private static final DecimalFormat FORMATTER = new DecimalFormat("#,##0.##");
    private static final Pattern REGEX_MATCHER =
      Pattern.compile("((?:\\d+)|(?:\\d*\\.\\d+))\\s*((?:[T|G|M|K])?B?)?", Pattern.CASE_INSENSITIVE);

  /**
   * Given a memory value in bytes, convert it to a string with the unit that round to a >0 integer part.
   *
   * @param value The memory value in long bytes
   * @return The formatted string, null if
   */
  public static String bytesToString(long value) {
    if (value < 0) {
      throw new IllegalArgumentException("Invalid memory size: " + value);
    }
    for (int i = 0; i < DIVIDERS.length; i++) {
      if (value >= DIVIDERS[i]) {
        double numResult = DIVIDERS[i] > 1 ? (double) value / (double) DIVIDERS[i] : (double) value;
        return FORMATTER.format(numResult) + " " + UNITS[i];
      }
    }
    return value + " " + UNITS[UNITS.length - 1];
  }

  /**
   * Convert a formatted string into a long value in bytes.
   * This method handles
   *
   * @param formattedString The string to convert
   * @return The bytes value
   */
  public static long stringToBytes(String formattedString) {
    if (formattedString == null) {
      return 0L;
    }

    Matcher matcher = REGEX_MATCHER.matcher(formattedString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "The formatted string [" + formattedString + "] does not match with the regex /" + REGEX_MATCHER.toString()
              + "/");
    }
    if (matcher.groupCount() != 1 && matcher.groupCount() != 2) {
      throw new IllegalArgumentException();
    }

    double numPart = Double.parseDouble(matcher.group(1));
    if (numPart < 0) {
      throw new IllegalArgumentException("The number part of the memory cannot be less than zero: [" + numPart + "].");
    }
    String unitPart = matcher.groupCount() == 2 ? matcher.group(2).toUpperCase() : "";
    if (!unitPart.endsWith("B")) {
      unitPart += "B";
    }
    for (int i = 0; i < UNITS.length; i++) {
      if (unitPart.equals(UNITS[i])) {
        return (long) (numPart * DIVIDERS[i]);
      }
    }
    throw new IllegalArgumentException("The formatted string [" + formattedString + "] 's unit part [" + unitPart
        + "] does not match any unit. The supported units are (case-insensitive, and also the 'B' is ignorable): ["
        + StringUtils.join(UNITS) + "].");
  }
}
