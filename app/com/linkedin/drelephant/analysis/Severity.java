/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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

  public static Severity getSeverityAscending(Number value, Number low, Number moderate, Number severe, Number critical) {
    if (value.doubleValue() >= critical.doubleValue()) {
      return CRITICAL;
    }
    if (value.doubleValue() >= severe.doubleValue()) {
      return SEVERE;
    }
    if (value.doubleValue() >= moderate.doubleValue()) {
      return MODERATE;
    }
    if (value.doubleValue() >= low.doubleValue()) {
      return LOW;
    }
    return NONE;
  }

  public static Severity getSeverityDescending(Number value, Number low, Number moderate, Number severe, Number critical) {
    if (value.doubleValue() <= critical.doubleValue()) {
      return CRITICAL;
    }
    if (value.doubleValue() <= severe.doubleValue()) {
      return SEVERE;
    }
    if (value.doubleValue() <= moderate.doubleValue()) {
      return MODERATE;
    }
    if (value.doubleValue() <= low.doubleValue()) {
      return LOW;
    }
    return NONE;
  }
}
