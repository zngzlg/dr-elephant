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

package model;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.util.Utils;

import play.db.ebean.Model;


@Entity
public class JobHeuristicResult extends Model {

  private static final long serialVersionUID = 123L;

  public static class TABLE {
    public static final String TABLE_NAME = "job_heuristic_result";
    public static final String ID = "id";
    public static final String JOB_JOB_ID = "job_job_id";
    public static final String SEVERITY = "severity";
    public static final String ANALYSIS_NAME = "analysis_name";
    public static final String DATA_COLUMNS = "data_columns";
    public static final String DATA = "data";
    public static final String TABLE_COLUMNS[] = {
      "job_heuristic_result.id",
      "job_heuristic_result.job_job_id",
      "job_heuristic_result.severity",
      "job_heuristic_result.analysis_name",
      "job_heuristic_result.data_columns",
      "job_heuristic_result.data"
    };
  }

  public static String getColumnList() {
    return StringUtils.join(TABLE.TABLE_COLUMNS, ',');
  }

  @JsonIgnore
  @Id
  public int id;

  @JsonBackReference
  @ManyToOne(cascade = CascadeType.ALL)
  public JobResult job;

  @Column
  public Severity severity;

  @Column
  public String analysisName;

  @JsonIgnore
  @Lob
  public String data;

  @JsonIgnore
  @Column
  public int dataColumns;

  public String[][] getDataArray() {
    return Utils.parseCsvLines(data);
  }
}
