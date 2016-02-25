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

package controllers;

import model.JobHeuristicResult;
import model.JobResult;

class QueryHandler {

  static StringBuilder getJobResultQuery() {
    final StringBuilder sqlQueryBuilder = new StringBuilder();
    sqlQueryBuilder.append("SELECT " + JobResult.getColumnList());
    sqlQueryBuilder.append(" FROM " + JobResult.TABLE.TABLE_NAME);
    return sqlQueryBuilder;
  }

  static StringBuilder getJobResultQueryWithUsernameIndex() {
    StringBuilder sqlQueryBuilder = getJobResultQuery();
    setUseIndex(sqlQueryBuilder, JobResult.TABLE.USERNAME_INDEX);
    return sqlQueryBuilder;
  }

  static StringBuilder getSqlJoinQuery() {
    StringBuilder sqlQueryBuilder = getJobResultQuery();
    sqlQueryBuilder.append(" JOIN " + JobHeuristicResult.TABLE.TABLE_NAME);
    sqlQueryBuilder.append(" ON " + JobHeuristicResult.TABLE.TABLE_NAME + "." + JobHeuristicResult.TABLE.JOB_JOB_ID
        + " = " + JobResult.TABLE.TABLE_NAME + "." + JobResult.TABLE.JOB_ID);
    return sqlQueryBuilder;
  }

  static StringBuilder getSqlJoinQueryWithUsernameIndex() {
    StringBuilder sqlQueryBuilder = getJobResultQuery();
    sqlQueryBuilder = setUseIndex(sqlQueryBuilder, JobResult.TABLE.USERNAME_INDEX);
    sqlQueryBuilder.append(" JOIN " + JobHeuristicResult.TABLE.TABLE_NAME);
    sqlQueryBuilder.append(" ON " + JobHeuristicResult.TABLE.TABLE_NAME + "." + JobHeuristicResult.TABLE.JOB_JOB_ID
        + " = " + JobResult.TABLE.TABLE_NAME + "." + JobResult.TABLE.JOB_ID);
    return sqlQueryBuilder;
  }

  private static StringBuilder setUseIndex(StringBuilder sqlQueryBuilder, String index) {
    return sqlQueryBuilder.append(" USE INDEX ( " + index + ")");
  }

}
