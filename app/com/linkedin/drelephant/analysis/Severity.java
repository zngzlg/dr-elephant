package com.linkedin.drelephant.analysis;

import com.avaje.ebean.annotation.EnumValue;


public enum Severity {
  @EnumValue("4")
  CRITICAL(4, "Critical", "danger"),

  @EnumValue("3")
  SEVERE(3, "Severe", "severe"),

  @EnumValue("2")
  MODERATE(2, "Moderate", "warning"),

  @EnumValue("1")
  LOW(1, "Low", "success"),

  @EnumValue("0")
  NONE(0, "None", "success");

  private int _value;
  private String _text;
  private String _bootstrapColor;

  Severity(int value, String text, String bootstrapColor) {
    this._value = value;
    this._text = text;
    this._bootstrapColor = bootstrapColor;
  }

  public int getValue() {
    return _value;
  }

  public String getText() {
    return _text;
  }

  public String getBootstrapColor() {
    return _bootstrapColor;
  }

  public static Severity byValue(int value) {
    for (Severity severity : values()) {
      if (severity._value == value) {
        return severity;
      }
    }
    return NONE;
  }

  public static Severity max(Severity a, Severity b) {
    if (a._value > b._value) {
      return a;
    }
    return b;
  }

  public static Severity max(Severity... severities) {
    Severity currentSeverity = NONE;
    for (Severity severity : severities) {
      currentSeverity = max(currentSeverity, severity);
    }
    return currentSeverity;
  }

  public static Severity min(Severity a, Severity b) {
    if (a._value < b._value) {
      return a;
    }
    return b;
  }

  public static Severity getSeverityAscending(long value, long low, long moderate, long severe, long critical) {
    if (value >= critical) {
      return CRITICAL;
    }
    if (value >= severe) {
      return SEVERE;
    }
    if (value >= moderate) {
      return MODERATE;
    }
    if (value >= low) {
      return LOW;
    }
    return NONE;
  }

  public static Severity getSeverityDescending(long value, long low, long moderate, long severe, long critical) {
    if (value <= critical) {
      return CRITICAL;
    }
    if (value <= severe) {
      return SEVERE;
    }
    if (value <= moderate) {
      return MODERATE;
    }
    if (value <= low) {
      return LOW;
    }
    return NONE;
  }
}
