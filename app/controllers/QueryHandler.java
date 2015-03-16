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
