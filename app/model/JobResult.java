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

package model;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.linkedin.drelephant.analysis.Severity;

import play.db.ebean.Model;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;

import org.apache.commons.lang.StringUtils;


@Entity
public class JobResult extends Model {

  private static final long serialVersionUID = 1L;
  public static final int URL_LEN_LIMIT = 2048;

  public static class TABLE {
    public static final String TABLE_NAME = "job_result";
    public static final String JOB_ID = "job_id";
    public static final String USERNAME = "username";
    public static final String JOB_NAME = "job_name";
    public static final String START_TIME = "start_time";
    public static final String ANALYSIS_TIME = "analysis_time";
    public static final String SEVERITY = "severity";
    public static final String JOB_TYPE = "job_type";
    public static final String URL = "url";
    public static final String CLUSTER = "cluster";
    public static final String JOB_EXEC_URL = "job_exec_url";
    public static final String JOB_URL = "job_url";
    public static final String FLOW_EXEC_URL = "flow_exec_url";
    public static final String FLOW_URL = "flow_url";
    public static final String[] TABLE_COLUMNS = {
      "job_result.job_id",
      "job_result.username",
      "job_result.job_name",
      "job_result.start_time",
      "job_result.analysis_time",
      "job_result.severity",
      "job_result.job_type",
      "job_result.url",
      "job_result.cluster",
      "job_result.job_exec_url",
      "job_result.job_url",
      "job_result.flow_exec_url",
      "job_result.flow_url"
    };
    public static final String USERNAME_INDEX = "ix_job_result_username_1";
  }

  public static String getColumnList() {
    return StringUtils.join(TABLE.TABLE_COLUMNS, ',');
  }

  @Id
  @Column(length = 50)
  public String jobId;

  @Column(length = 50)
  public String username;

  @Column(length = 100)
  public String jobName;

  @Column
  public long startTime;

  @Column
  public long analysisTime;

  @Column
  public Severity severity;

  @Column
  public String jobType;

  @Column(length = 200)
  public String url;

  @Column(length = 100)
  public String cluster;

  @Column(length = URL_LEN_LIMIT)
  public String jobExecUrl;

  @Column(length = URL_LEN_LIMIT)
  public String jobUrl;

  @Column(length = URL_LEN_LIMIT)
  public String flowExecUrl;

  @Column(length = URL_LEN_LIMIT)
  public String flowUrl;

  @JsonManagedReference
  @OneToMany(cascade = CascadeType.ALL, mappedBy = "job")
  public List<JobHeuristicResult> heuristicResults;

  public static Finder<String, JobResult> find = new Finder<String, JobResult>(String.class, JobResult.class);
}
