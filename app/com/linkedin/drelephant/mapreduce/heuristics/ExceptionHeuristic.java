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

package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;


public class ExceptionHeuristic implements Heuristic<MapReduceApplicationData> {

  public static final String HEURISTIC_NAME = "Exception";
  private HeuristicConfigurationData _heuristicConfData;

  public ExceptionHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    if (data.getSucceeded()) {
      return null;
    }
    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, Severity.MODERATE);
    String diagnosticInfo = data.getDiagnosticInfo();
    if (diagnosticInfo != null) {
      result.addDetail(diagnosticInfo);
    } else {
      String msg = "Unable to find stacktrace info. Please find the real problem in the Jobhistory link above.\n"
          + "Exception can happen either in task log or Application Master log.";
      result.addDetail(msg);
    }
    return result;
  }
}
