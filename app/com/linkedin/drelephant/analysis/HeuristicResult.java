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

    /**
     * Gets a list of lines of comma-separated strings
     * @return
     */
    public List<String> getDetails() {
        return details;
    }

    /**
     * Create a string that contains lines of comma-separated strings
     * @return
     */
    public String getDetailsCSV() {
        StringBuilder sb = new StringBuilder();
        for (String line : details) {
            sb.append(line).append("\n");
        }
        return sb.toString().trim();
    }

    /**
     * Gets the number of columns in the csv formatted details store
     * @return
     */
    public int getDetailsColumns() {
        return detailsColumns;
    }

    /**
     * Add a new line to the csv formatted details store
     * @param parts strings to join into a single line
     */
    public void addDetail(String... parts) {
        details.add(createLine(parts));
        if (parts.length > detailsColumns) {
            detailsColumns = parts.length;
        }
    }

    /**
     * Create a comma-separated line from a list of strings
     * @param parts strings in each cell of the csv
     * @return
     */
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
