package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.util.Utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;


public class HeuristicResult {
  public static final HeuristicResult NO_DATA = new HeuristicResult("No Data Received", Severity.LOW);

  private String _analysis;
  private Severity _severity;
  private List<String> _details;
  private int _detailsColumns = 0;

  public HeuristicResult(String analysis, Severity severity) {
    this._analysis = analysis;
    this._severity = severity;
    this._details = new ArrayList<String>();
  }

  public String getAnalysis() {
    return _analysis;
  }

  public Severity getSeverity() {
    return _severity;
  }

  /**
   * Gets a list of lines of comma-separated strings
   *
   * @return
   */
  public List<String> getDetails() {
    return _details;
  }

  /**
   * Create a string that contains lines of comma-separated strings
   *
   * @return
   */
  public String getDetailsCSV() {
    return Utils.combineCsvLines(_details.toArray(new String[_details.size()]));
  }

  /**
   * Gets the number of columns in the csv formatted details store
   *
   * @return
   */
  public int getDetailsColumns() {
    return _detailsColumns;
  }

  /**
   * Add a new line to the csv formatted details store
   *
   * @param parts strings to join into a single line
   */
  public void addDetail(String... parts) {
    _details.add(Utils.createCsvLine(parts));
    if (parts.length > _detailsColumns) {
      _detailsColumns = parts.length;
    }
  }

  public void setSeverity(Severity severity) {
    this._severity = severity;
  }

  @Override
  public String toString() {
    return "{analysis: " + _analysis + ", severity: " + _severity + ", details: [" + StringUtils.join(_details, "    ")
        + "]}";
  }
}
