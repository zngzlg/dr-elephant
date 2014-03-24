package com.linkedin.drelephant.analysis;

import java.util.ArrayList;
import java.util.List;

public class HeuristicResult {
    public static final HeuristicResult SUCCESS = new HeuristicResult("Everything looks good", true);

    private String message;
    private List<String> details;
    private int detailsColumns = 0;
    private boolean success;

    public HeuristicResult(String message, boolean success) {
        this.message = message;
        this.details = new ArrayList<String>();
        this.success = success;
    }

    public boolean succeeded() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public List<String> getDetails() {
        return details;
    }

    public String getDetailsCSV() {
        StringBuilder sb = new StringBuilder();
        for (String line : details) {
            sb.append(line).append("\n");
        }
        return sb.toString().trim();
    }

    public int getDetailsColumns() {
        return detailsColumns;
    }

    public void addDetail(String... parts) {
        details.add(createLine(parts));
        if (parts.length > detailsColumns) {
            detailsColumns = parts.length;
        }
    }

    public static String createLine(String... parts) {
        StringBuilder sb = new StringBuilder();
        String quotes = "\"";
        String comma = ",";
        for (int i = 0; i < parts.length; i++) {
            sb.append(quotes).append(parts[i].replaceAll("\"", "\\\"")).append(quotes);
            if (i != parts.length - 1) {
                sb.append(comma);
            }
        }
        return sb.toString();
    }
}
