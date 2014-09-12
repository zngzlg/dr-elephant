package com.linkedin.drelephant.util;

import java.util.ArrayList;
import java.util.List;


public final class Utils {

  private Utils() {
  }

  public static String combineCsvLines(String[] lines) {
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(line).append("\n");
    }
    return sb.toString().trim();
  }

  public static String createCsvLine(String... parts) {
    StringBuilder sb = new StringBuilder();
    String quotes = "\"";
    String comma = ",";
    for (int i = 0; i < parts.length; i++) {
      sb.append(quotes).append(parts[i].replaceAll(quotes, quotes + quotes)).append(quotes);
      if (i != parts.length - 1) {
        sb.append(comma);
      }
    }
    return sb.toString();
  }

  public static String[][] parseCsvLines(String data) {
    if (data.isEmpty()) {
      return new String[0][];
    }
    String[] lines = data.split("\n");
    String[][] result = new String[lines.length][];
    for (int i = 0; i < lines.length; i++) {
      result[i] = parseCsvLine(lines[i]);
    }
    return result;
  }

  public static String[] parseCsvLine(String line) {
    List<String> store = new ArrayList<String>();
    StringBuilder curVal = new StringBuilder();
    boolean inquotes = false;
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (inquotes) {
        if (ch == '\"') {
          inquotes = false;
        } else {
          curVal.append(ch);
        }
      } else {
        if (ch == '\"') {
          inquotes = true;
          if (curVal.length() > 0) {
            //if this is the second quote in a value, add a quote
            //this is for the double quote in the middle of a value
            curVal.append('\"');
          }
        } else if (ch == ',') {
          store.add(curVal.toString());
          curVal = new StringBuilder();
        } else {
          curVal.append(ch);
        }
      }
    }
    store.add(curVal.toString());
    return store.toArray(new String[store.size()]);
  }
}
