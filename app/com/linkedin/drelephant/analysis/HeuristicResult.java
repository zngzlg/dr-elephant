/*
 * Copyright 2016 LinkedIn Corp.
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

import com.linkedin.drelephant.util.Utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;


/**
 * Holds the Heuristic analysis result Information
 */
public class HeuristicResult {
  public static final HeuristicResult NO_DATA = new HeuristicResult("No Data Received", Severity.LOW);

  private String _analysis;
  private Severity _severity;
  private List<String> _details;
  private int _detailsColumns = 0;

  /**
   * Heuristic Result Constructor
   *
   * @param analysis The name of the heuristic
   * @param severity The severity level of the heuristic
   */
  public HeuristicResult(String analysis, Severity severity) {
    this._analysis = analysis;
    this._severity = severity;
    this._details = new ArrayList<String>();
  }

  /**
   * Returns the heuristic analyser name
   *
   * @return the analysis name
   */
  public String getAnalysis() {
    return _analysis;
  }

  /**
   * Returns the severity of the Heuristic
   *
   * @return The severity
   */
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

  /**
   * Set the severity of the heuristic
   *
   * @param severity The severity to be set
   */
  public void setSeverity(Severity severity) {
    this._severity = severity;
  }

  @Override
  public String toString() {
    return "{analysis: " + _analysis + ", severity: " + _severity + ", details: [" + StringUtils.join(_details, "    ")
        + "]}";
  }
}
