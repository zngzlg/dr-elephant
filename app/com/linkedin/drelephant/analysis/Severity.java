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

    private int value;
    private String text;
    private String bootstrapColor;

    Severity(int value, String text, String bootstrapColor) {
        this.value = value;
        this.text = text;
        this.bootstrapColor = bootstrapColor;
    }

    public int getValue() {
        return value;
    }

    public String getText() {
        return text;
    }

    public String getBootstrapColor() {
        return bootstrapColor;
    }

    public static Severity byValue(int value) {
        for (Severity severity : values()) {
            if (severity.value == value) {
                return severity;
            }
        }
        return NONE;
    }

    public static Severity max(Severity a, Severity b) {
        if (a.value > b.value) {
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
        if (a.value < b.value) {
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
