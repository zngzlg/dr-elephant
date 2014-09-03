package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.util.Utils;

import java.util.ArrayList;
import java.util.List;

public class HeuristicResult {
    private String analysis;
    private Severity severity;
    private List<String> details;
    private int detailsColumns = 0;

    public HeuristicResult(String analysis, Severity severity) {
        this.analysis = analysis;
        this.severity = severity;
        this.details = new ArrayList<String>();
    }

    public String getAnalysis() {
        return analysis;
    }

    public Severity getSeverity() {
        return severity;
    }

    /**
     * Gets a list of lines of comma-separated strings
     *
     * @return
     */
    public List<String> getDetails() {
        return details;
    }

    /**
     * Create a string that contains lines of comma-separated strings
     *
     * @return
     */
    public String getDetailsCSV() {
        return Utils.combineCsvLines(details.toArray(new String[details.size()]));
    }

    /**
     * Gets the number of columns in the csv formatted details store
     *
     * @return
     */
    public int getDetailsColumns() {
        return detailsColumns;
    }

    /**
     * Add a new line to the csv formatted details store
     *
     * @param parts strings to join into a single line
     */
    public void addDetail(String... parts) {
        details.add(Utils.createCsvLine(parts));
        if (parts.length > detailsColumns) {
            detailsColumns = parts.length;
        }
    }

    public void setSeverity(Severity severity){
      this.severity = severity;
    }
}
