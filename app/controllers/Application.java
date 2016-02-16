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

package controllers;

import com.avaje.ebean.ExpressionList;
import com.avaje.ebean.RawSql;
import com.avaje.ebean.RawSqlBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import model.JobHeuristicResult;
import model.JobResult;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import play.api.Play;
import play.api.templates.Html;
import play.data.DynamicForm;
import play.data.Form;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.emailcritical;
import views.html.page.comparePage;
import views.html.page.flowHistoryPage;
import views.html.page.helpPage;
import views.html.page.homePage;
import views.html.page.jobHistoryPage;
import views.html.page.searchPage;
import views.html.results.compareResults;
import views.html.results.flowDetails;
import views.html.results.flowHistoryResults;
import views.html.results.jobDetails;
import views.html.results.jobHistoryResults;
import views.html.results.searchResults;
import com.google.gson.*;


public class Application extends Controller {
  private static final Logger logger = Logger.getLogger(Application.class);
  private static final long DAY = 24 * 60 * 60 * 1000;
  private static final long FETCH_DELAY = 60 * 1000;

  private static final int PAGE_LENGTH = 20;                  // Num of jobs in a search page
  private static final int PAGE_BAR_LENGTH = 5;               // Num of pages shown in the page bar
  private static final int REST_PAGE_LENGTH = 100;            // Num of jobs in a rest search page
  private static final int JOB_HISTORY_LIMIT = 5000;          // Set to avoid memory error.
  private static final int MAX_HISTORY_LIMIT = 15;            // Upper limit on the number of executions to display

  // Form and Rest parameters
  private static final String JOB_ID = "id";
  private static final String FLOW_URL = "flow-url";
  private static final String FLOW_EXEC_URL = "flow-exec-url";
  private static final String JOB_URL = "job-url";
  private static final String USER = "user";
  private static final String SEVERITY = "severity";
  private static final String JOB_TYPE = "job-type";
  private static final String ANALYSIS = "analysis";
  private static final String STARTED_TIME_BEGIN = "started-time-begin";
  private static final String STARTED_TIME_END = "started-time-end";
  private static final String FINISHED_TIME_BEGIN = "finished-time-begin";
  private static final String FINISHED_TIME_END = "finished-time-end";
  private static final String COMPARE_FLOW_URL1 = "flow-exec-url1";
  private static final String COMPARE_FLOW_URL2 = "flow-exec-url2";
  private static final String PAGE = "page";

  // Time range specifier. [TIME_RANGE_BEGIN, TIME_RANGE_END]
  private static final boolean TIME_RANGE_BEGIN = false;
  private static final boolean TIME_RANGE_END = true;

  private static long _lastFetch = 0;
  private static int _numJobsAnalyzed = 0;
  private static int _numJobsCritical = 0;
  private static int _numJobsSevere = 0;
  private static Map<String, Html> _helpPages = new HashMap<String, Html>();

  static {
    try {
      logger.info("Loading pluggable heuristics help pages.");
      fillHelpPages();
    } catch (Exception e) {
      logger.error("Error loading pluggable heuristics help pages.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Controls the Home page of Dr. Elephant
   */
  public static Result dashboard() {
    long now = System.currentTimeMillis();
    if (now - _lastFetch > FETCH_DELAY) {
      _numJobsAnalyzed = JobResult.find.where().gt(JobResult.TABLE.ANALYSIS_TIME, now - DAY).findRowCount();
      _numJobsCritical =
          JobResult.find.where().gt(JobResult.TABLE.ANALYSIS_TIME, now - DAY)
              .eq(JobResult.TABLE.SEVERITY, Severity.CRITICAL.getValue()).findRowCount();
      _numJobsSevere =
          JobResult.find.where().gt(JobResult.TABLE.ANALYSIS_TIME, now - DAY)
              .eq(JobResult.TABLE.SEVERITY, Severity.SEVERE.getValue()).findRowCount();
      _lastFetch = now;
    }
    List<JobResult> results =
        JobResult.find.where().gt(JobResult.TABLE.ANALYSIS_TIME, now - DAY).order().desc(JobResult.TABLE.ANALYSIS_TIME)
            .setMaxRows(50).fetch("heuristicResults").findList();

    return ok(homePage.render(_numJobsAnalyzed, _numJobsSevere, _numJobsCritical,
        searchResults.render("Latest analysis", results)));
  }

  /**
   * Controls the Search Feature
   */
  public static Result search() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String jobId = form.get(JOB_ID);
    jobId = jobId != null ? jobId.trim() : "";
    String flowUrl = form.get(FLOW_URL);
    flowUrl = (flowUrl != null) ? flowUrl.trim() : null;

    // Search and display job information when job id or flow execution url is provided.
    if (!jobId.isEmpty()) {
      JobResult result = JobResult.find.byId(jobId);
      if (result != null) {
        return ok(searchPage.render(null, jobDetails.render(result)));
      } else {
        return ok(searchPage.render(null, jobDetails.render(null)));
      }
    } else if (flowUrl != null && !flowUrl.isEmpty()) {
      List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.FLOW_EXEC_URL, flowUrl).findList();
      Map<String, List<JobResult>> map = groupJobs(results, GroupBy.JOB_EXECUTION_URL);
      return ok(searchPage.render(null, flowDetails.render(flowUrl, map)));
    }

    // Paginate the results
    PaginationStats paginationStats = new PaginationStats(PAGE_LENGTH, PAGE_BAR_LENGTH);
    int pageLength = paginationStats.getPageLength();
    paginationStats.setCurrentPage(1);
    final Map<String, String[]> searchString = request().queryString();
    if (searchString.containsKey(PAGE)) {
      try {
        paginationStats.setCurrentPage(Integer.parseInt(searchString.get(PAGE)[0]));
      } catch (NumberFormatException ex) {
        logger.error("Error parsing page number. Setting current page to 1.");
        paginationStats.setCurrentPage(1);
      }
    }
    int currentPage = paginationStats.getCurrentPage();
    int paginationBarStartIndex = paginationStats.getPaginationBarStartIndex();

    // Filter jobs by search parameters
    ExpressionList<JobResult> query = generateQuery();
    List<JobResult> results =
        query.order().desc(JobResult.TABLE.ANALYSIS_TIME).setFirstRow((paginationBarStartIndex - 1) * pageLength)
            .setMaxRows((paginationStats.getPageBarLength() - 1) * pageLength + 1).findList();
    paginationStats.setQueryString(getQueryString());
    if (results.isEmpty() || currentPage > paginationStats.computePaginationBarEndIndex(results.size())) {
      return ok(searchPage.render(null, jobDetails.render(null)));
    } else {
      return ok(searchPage.render(
          paginationStats,
          searchResults.render(
              "Results",
              results.subList((currentPage - paginationBarStartIndex) * pageLength,
                  Math.min(results.size(), (currentPage - paginationBarStartIndex + 1) * pageLength)))));
    }
  }

  /**
   * Parses the request for the queryString
   *
   * @return URL Encoded String of Parameter Value Pair
   */
  private static String getQueryString() {
    List<BasicNameValuePair> fields = new LinkedList<BasicNameValuePair>();
    final Set<Map.Entry<String, String[]>> entries = request().queryString().entrySet();
    for (Map.Entry<String, String[]> entry : entries) {
      final String key = entry.getKey();
      final String value = entry.getValue()[0];
      if (!key.equals(PAGE)) {
        fields.add(new BasicNameValuePair(key, value));
      }
    }
    if (fields.isEmpty()) {
      return null;
    } else {
      return URLEncodedUtils.format(fields, "utf-8");
    }
  }

  /**
   * Build SQL expression
   *
   * @return An sql expression on Job Result
   */
  private static ExpressionList<JobResult> generateQuery() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String username = form.get(USER);
    username = username != null ? username.trim().toLowerCase() : null;
    String severity = form.get(SEVERITY);
    String jobType = form.get(JOB_TYPE);
    String analysis = form.get(ANALYSIS);
    String finishedTimeBegin = form.get(FINISHED_TIME_BEGIN);
    String finishedTimeEnd = form.get(FINISHED_TIME_END);
    String startedTimeBegin = form.get(STARTED_TIME_BEGIN);
    String startedTimeEnd = form.get(STARTED_TIME_END);

    ExpressionList<JobResult> query = JobResult.find.where();

    RawSql rawsql = null;
    // Hint usage of username index to mysql whenever our query contains a predicate on username
    if (isSet(severity) && isSet(analysis)) {
      if (isSet(username)) {
        rawsql = RawSqlBuilder.parse(QueryHandler.getSqlJoinQueryWithUsernameIndex().toString()).create();
      } else {
        rawsql = RawSqlBuilder.parse(QueryHandler.getSqlJoinQuery().toString()).create();
      }
    } else {
      if (isSet(username)) {
        rawsql = RawSqlBuilder.parse(QueryHandler.getJobResultQueryWithUsernameIndex().toString()).create();
      }
    }
    query = query.query().setRawSql(rawsql).where();

    // Build predicates
    if (isSet(username)) {
      query = query.like(JobResult.TABLE.USERNAME, username);
    }
    if (isSet(jobType)) {
      query = query.eq(JobResult.TABLE.JOB_TYPE, jobType);
    }
    if (isSet(severity)) {
      if (isSet(analysis)) {
        query =
            query.eq(JobHeuristicResult.TABLE.TABLE_NAME + "." + JobHeuristicResult.TABLE.ANALYSIS_NAME, analysis).ge(
                JobHeuristicResult.TABLE.TABLE_NAME + "." + JobHeuristicResult.TABLE.SEVERITY, severity);
      } else {
        query = query.ge(JobResult.TABLE.SEVERITY, severity);
      }
    }

    // Time Predicates. Both the startedTimeBegin and startedTimeEnd are inclusive in the filter
    if (isSet(startedTimeBegin)) {
      long time = parseTime(startedTimeBegin);
      if (time > 0) {
        query = query.ge(JobResult.TABLE.ANALYSIS_TIME, time);
      }
    }
    if (isSet(startedTimeEnd)) {
      long time = parseTime(startedTimeEnd);
      if (time > 0) {
        query = query.le(JobResult.TABLE.ANALYSIS_TIME, time);
      }
    }
    if (isSet(finishedTimeBegin)) {
      long time = parseTime(finishedTimeBegin);
      if (time > 0) {
        query = query.ge(JobResult.TABLE.ANALYSIS_TIME, time);
      }
    }
    if (isSet(finishedTimeEnd)) {
      long time = parseTime(finishedTimeEnd);
      if (time > 0) {
        query = query.le(JobResult.TABLE.ANALYSIS_TIME, time);
      }
    }

    return query;
  }

  /**
   Controls the Compare Feature
   */
  public static Result compare() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String flowExecUrl1 = form.get(COMPARE_FLOW_URL1);
    flowExecUrl1 = (flowExecUrl1 != null) ? flowExecUrl1.trim() : null;
    String flowExecUrl2 = form.get(COMPARE_FLOW_URL2);
    flowExecUrl2 = (flowExecUrl2 != null) ? flowExecUrl2.trim() : null;
    return ok(comparePage.render(compareResults.render(compareFlows(flowExecUrl1, flowExecUrl2))));
  }

  /**
   * Helper Method for the compare controller.
   * This Compares 2 flow executions at job level.
   *
   * @param flowExecUrl1 The flow execution url to be compared
   * @param flowExecUrl2 The other flow execution url to be compared against
   * @return A map of Job Urls to the list of jobs corresponding to the 2 flow execution urls
   */
  private static Map<String, Map<String, List<JobResult>>> compareFlows(String flowExecUrl1, String flowExecUrl2) {
    Map<String, Map<String, List<JobResult>>> jobDefMap = new HashMap<String, Map<String, List<JobResult>>>();

    if (flowExecUrl1 != null && !flowExecUrl1.isEmpty() && flowExecUrl2 != null && !flowExecUrl2.isEmpty()) {
      List<JobResult> results1 = JobResult.find.where().eq(JobResult.TABLE.FLOW_EXEC_URL, flowExecUrl1).findList();
      List<JobResult> results2 = JobResult.find.where().eq(JobResult.TABLE.FLOW_EXEC_URL, flowExecUrl2).findList();

      Map<String, List<JobResult>> map1 = groupJobs(results1, GroupBy.JOB_DEFINITION_URL);
      Map<String, List<JobResult>> map2 = groupJobs(results2, GroupBy.JOB_DEFINITION_URL);

      // We want to display jobs that are common to the two flows first and then display jobs in flow 1 and flow 2.
      Set<String> CommonFlows = Sets.intersection(map1.keySet(), map2.keySet());
      Set<String> orderedFlowSet = Sets.union(CommonFlows, map1.keySet());
      Set<String> union = Sets.union(orderedFlowSet, map2.keySet());

      for (String jobDefUrl : union) {
        Map<String, List<JobResult>> flowExecMap = new LinkedHashMap<String, List<JobResult>>();
        flowExecMap.put(flowExecUrl1, map1.get(jobDefUrl));
        flowExecMap.put(flowExecUrl2, map2.get(jobDefUrl));
        jobDefMap.put(jobDefUrl, flowExecMap);
      }
    }
    return jobDefMap;
  }

  /**
   * Controls the flow history. Displays max MAX_HISTORY_LIMIT executions
   */
  public static Result flowHistory() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String flowUrl = form.get(FLOW_URL);
    flowUrl = (flowUrl != null) ? flowUrl.trim() : null;
    if (flowUrl == null || flowUrl.isEmpty()) {
      return ok(flowHistoryPage.render(flowHistoryResults.render(null, null, null, null)));
    }

    // Fetch available flow executions with latest JOB_HISTORY_LIMIT mr jobs.
    List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.FLOW_URL, flowUrl).order()
        .desc(JobResult.TABLE.ANALYSIS_TIME).setMaxRows(JOB_HISTORY_LIMIT).findList();
    if (results.size() == 0) {
      return notFound("Unable to find record on flow url: " + flowUrl);
    }
    Map<String, List<JobResult>> flowExecUrlToJobsMap =  limitHistoryResults(
        groupJobs(results, GroupBy.FLOW_EXECUTION_URL), results.size(), MAX_HISTORY_LIMIT);

    // Compute flow execution data
    List<JobResult> filteredResults = new ArrayList<JobResult>();     // All mr jobs starting from latest execution
    List<Long> flowExecTimeList = new ArrayList<Long>();              // To map executions to resp execution time
    Map<String, Map<String, List<JobResult>>> executionMap = new LinkedHashMap<String, Map<String, List<JobResult>>>();
    for (Map.Entry<String, List<JobResult>> entry: flowExecUrlToJobsMap.entrySet()) {

      // Reverse the list content from desc order of analysis_time to increasing order so that when grouping we get
      // the job list in the order of completion.
      List<JobResult> mrJobsList = Lists.reverse(entry.getValue());

      // Flow exec time is the analysis_time of the last mr job in the flow
      flowExecTimeList.add(mrJobsList.get(mrJobsList.size() - 1).analysisTime);

      filteredResults.addAll(mrJobsList);
      executionMap.put(entry.getKey(), groupJobs(mrJobsList, GroupBy.JOB_DEFINITION_URL));
    }

    // Calculate unique list of jobs (job def url) to maintain order across executions. List will contain job def urls
    // from latest execution first followed by any other extra job def url that may appear in previous executions.
    List<String> jobDefUrlList = new ArrayList<String>(groupJobs(filteredResults, GroupBy.JOB_DEFINITION_URL).keySet());

    return ok(flowHistoryPage.render(flowHistoryResults.render(flowUrl, executionMap, jobDefUrlList,
        flowExecTimeList)));
  }

  /**
   * Controls Job History. Displays at max MAX_HISTORY_LIMIT executions
   */
  public static Result jobHistory() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String jobUrl = form.get(JOB_URL);
    jobUrl = (jobUrl != null) ? jobUrl.trim() : null;
    if (jobUrl == null || jobUrl.isEmpty()) {
      return ok(jobHistoryPage.render(jobHistoryResults.render(null, null, -1, null)));
    }

    // Fetch all job executions
    List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.JOB_URL, jobUrl).order()
        .desc(JobResult.TABLE.ANALYSIS_TIME).setMaxRows(JOB_HISTORY_LIMIT).findList();
    if (results.size() == 0) {
      return notFound("Unable to find record on job url: " + jobUrl);
    }
    Map<String, List<JobResult>> flowExecUrlToJobsMap =
        limitHistoryResults(groupJobs(results, GroupBy.FLOW_EXECUTION_URL), results.size(), MAX_HISTORY_LIMIT);

    // Compute job execution data
    List<Long> flowExecTimeList = new ArrayList<Long>();
    int maxStages = 0;
    Map<String, List<JobResult>> executionMap = new LinkedHashMap<String, List<JobResult>>();
    for (Map.Entry<String, List<JobResult>> entry: flowExecUrlToJobsMap.entrySet()) {

      // Reverse the list content from desc order of analysis_time to increasing order so that when grouping we get
      // the job list in the order of completion.
      List<JobResult> mrJobsList = Lists.reverse(entry.getValue());

      // Get the analysis_time of the last mr job that completed in current flow.
      flowExecTimeList.add(mrJobsList.get(mrJobsList.size() - 1).analysisTime);

      // Find the maximum number of mr stages for any job execution
      int stageSize = flowExecUrlToJobsMap.get(entry.getKey()).size();
      if (stageSize > maxStages) {
        maxStages = stageSize;
      }

      executionMap.put(entry.getKey(), Lists.reverse(flowExecUrlToJobsMap.get(entry.getKey())));
    }

    return ok(jobHistoryPage.render(jobHistoryResults.render(jobUrl, executionMap, maxStages, flowExecTimeList)));
  }

  /**
   * Applies a limit on the number of executions to be displayed after trying to maximize the correctness.
   *
   * Correctness:
   * When the number of jobs are less than the JOB_HISTORY_LIMIT, we can show all the executions correctly. However,
   * when the number of jobs are greater than the JOB_HISTORY_LIMIT, we cannot simply prune the jobs at that point and
   * show the history because we may skip some jobs which belong to the last flow execution. For the flow executions
   * we display, we want to ensure we show all the jobs belonging to that flow.
   *
   * So, when the number of executions are less than 10, we skip the last execution and when the number of executions
   * are greater than 10, we skip the last 3 executions just to maximise the correctness.
   *
   * @param map The results map to be pruned.
   * @param size Total number of jobs in the map
   * @param execLimit The upper limit on the number of executions to be displayed.
   * @return A map after applying the limit.
   */
  private static Map<String, List<JobResult>> limitHistoryResults(Map<String, List<JobResult>> map, int size,
      int execLimit) {
    Map<String, List<JobResult>> resultMap = new LinkedHashMap<String, List<JobResult>>();

    int limit;
    if (size < JOB_HISTORY_LIMIT) {
      // No pruning needed. 100% correct.
      limit = execLimit;
    } else {
      Set<String> keySet = map.keySet();
      if (keySet.size() > 10) {
        // Prune last 3 executions
        limit = keySet.size() > (execLimit + 3) ? execLimit : keySet.size() - 3;
      } else {
        // Prune the last execution
        limit = keySet.size() - 1;
      }
    }

    // Filtered results
    int i = 1;
    for (Map.Entry<String, List<JobResult>> entry : map.entrySet()) {
      if (i > limit) {
        break;
      }
      resultMap.put(entry.getKey(), entry.getValue());
      i++;
    }

    return resultMap;
  }

  /**
   * Controls the Help Page
   */
  public static Result help() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String topic = form.get("topic");
    Html page = null;
    String title = "Help";
    if (topic != null && !topic.isEmpty()) {
      page = _helpPages.get(topic);
      if (page != null) {
        title = topic;
      }
    }
    return ok(helpPage.render(title, page));
  }

  /**
   * Create a map to cache pages.
   */
  private static void fillHelpPages() {
    logger.info("Loading help pages for pluggable heuristics");
    List<HeuristicConfigurationData> heuristicsConfList = ElephantContext.instance().getHeuristicsConfigurationData();
    for (HeuristicConfigurationData heuristicConf : heuristicsConfList) {
      Class<?> viewClass = null;
      String heuristicName = null;
      try {
        String viewName = heuristicConf.getViewName();
        logger.info("Loading help page " + viewName);
        viewClass = Play.current().classloader().loadClass(viewName);
        heuristicName = heuristicConf.getHeuristicName();
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not find class " + heuristicConf.getViewName(), e);
      }

      try {
        Method render = viewClass.getDeclaredMethod("render");
        Html page = (Html) render.invoke(null);
        _helpPages.put(heuristicName, page);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(viewClass.getName() + " is not a valid view.", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(viewClass.getName() + " is not a valid view.", e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(viewClass.getName() + " is not a valid view.", e);
      } catch (Exception e) {
        // More descriptive on other Runtime Exceptions such as ClassCastException IllegalArgumentException
        throw new RuntimeException(viewClass.getName() + " is not a valid view.", e);
      }
    }
  }

  /**
   * Parse the string for time in long
   *
   * @param time The string to be parsed
   * @return the epoch value
   */
  private static long parseTime(String time) {
    long unixTime = 0;
    try {
      unixTime = Long.parseLong(time);
    } catch (NumberFormatException ex) {
      // return 0
    }
    return unixTime;
  }

  /**
   * Checks if the property is set
   *
   * @param property The property to tbe checked.
   * @return true if set, false otherwise
   */
  private static boolean isSet(String property) {
    return property != null && !property.isEmpty();
  }

  /**
   * Rest API for searching a particular job information
   */
  public static Result restJobResult(String jobId) {

    if (jobId == null || jobId.isEmpty()) {
      return badRequest("No job id provided.");
    }

    JobResult result = JobResult.find.byId(jobId);

    if (result == null) {
      return notFound("Unable to find record on job id: " + jobId);
    }

    return ok(Json.toJson(result));
  }

  /**
   * Rest API for searching all jobs triggered by a particular Scheduler Job
   */
  public static Result restJobExecResult(String jobExecUrl) {

    if (jobExecUrl == null || jobExecUrl.isEmpty()) {
      return badRequest("No job exec url provided.");
    }

    List<JobResult> result = JobResult.find.where().eq(JobResult.TABLE.JOB_EXEC_URL, jobExecUrl).findList();

    if (result.size() == 0) {
      return notFound("Unable to find record on job exec url: " + jobExecUrl);
    }

    return ok(Json.toJson(result));
  }

  /**
   * Rest API for searching all jobs under a particular flow execution
   */
  public static Result restFlowExecResult(String flowExecUrl) {

    if (flowExecUrl == null || flowExecUrl.isEmpty()) {
      return badRequest("No flow exec url provided.");
    }

    List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.FLOW_EXEC_URL, flowExecUrl).findList();

    if (results.size() == 0) {
      return notFound("Unable to find record on flow exec url: " + flowExecUrl);
    }

    Map<String, List<JobResult>> resMap = groupJobs(results, GroupBy.JOB_EXECUTION_URL);

    return ok(Json.toJson(resMap));
  }

  static enum GroupBy {
    JOB_EXECUTION_URL,
    JOB_DEFINITION_URL,
    FLOW_EXECUTION_URL
  }

  /**
   * Grouping a list of JobResult by GroupBy enum.
   *
   * @param results The list of jobs of type JobResult to be grouped.
   * @param groupBy The field by which the results have to be grouped.
   * @return A map with the grouped field as the key and the list of jobs as the value.
   */
  private static Map<String, List<JobResult>> groupJobs(List<JobResult> results, GroupBy groupBy) {

    Map<String, List<JobResult>> resultMap = new LinkedHashMap<String, List<JobResult>>();

    for (JobResult result : results) {
      String field = null;
      switch (groupBy) {
        case JOB_EXECUTION_URL:
          field = result.jobExecUrl;
          break;
        case JOB_DEFINITION_URL:
          field = result.jobUrl;
          break;
        case FLOW_EXECUTION_URL:
          field = result.flowExecUrl;
          break;
      }

      if (resultMap.containsKey(field)) {
        resultMap.get(field).add(result);
      } else {
        List<JobResult> list = new ArrayList<JobResult>();
        list.add(result);
        resultMap.put(field, list);
      }
    }
    return resultMap;
  }

  /**
   * The Rest API for Search Feature
   *
   * http://localhost:8080/rest/search?username=abc&job-type=HadoopJava
   */
  public static Result restSearch() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String jobId = form.get(JOB_ID);
    jobId = jobId != null ? jobId.trim() : "";
    String flowUrl = form.get(FLOW_URL);
    flowUrl = (flowUrl != null) ? flowUrl.trim() : null;
    if (!jobId.isEmpty()) {
      JobResult result = JobResult.find.byId(jobId);
      if (result != null) {
        return ok(Json.toJson(result));
      } else {
        return notFound("Unable to find record on job id: " + jobId);
      }
    } else if (flowUrl != null && !flowUrl.isEmpty()) {
      List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.FLOW_EXEC_URL, flowUrl).findList();
      return ok(Json.toJson(results));
    }

    int page = 1;
    if (request().queryString().containsKey(PAGE)) {
      page = Integer.parseInt(request().queryString().get(PAGE)[0]);
      if (page <= 0) {
        page = 1;
      }
    }

    ExpressionList<JobResult> query = generateQuery();
    List<JobResult> results =
        query.order().desc(JobResult.TABLE.ANALYSIS_TIME).setFirstRow((page - 1) * REST_PAGE_LENGTH)
            .setMaxRows(REST_PAGE_LENGTH).findList();
    return ok(Json.toJson(results));
  }

  /**
   * The Rest API for Compare Feature
   */
  public static Result restCompare() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String flowExecUrl1 = form.get(COMPARE_FLOW_URL1);
    flowExecUrl1 = (flowExecUrl1 != null) ? flowExecUrl1.trim() : null;
    String flowExecUrl2 = form.get(COMPARE_FLOW_URL2);
    flowExecUrl2 = (flowExecUrl2 != null) ? flowExecUrl2.trim() : null;
    return ok(Json.toJson(compareFlows(flowExecUrl1, flowExecUrl2)));
  }

  /**
   * The data for plotting the flow history graph
   *
   * <pre>
   * {@code
   *   [
   *     {
   *       "flowtime": <Last job's analysis_time>,
   *       "score": 1000,
   *       "jobscores": [
   *         {
   *           "jobdefurl:" "url",
   *           "jobscore": 500
   *         },
   *         {
   *           "jobdefurl:" "url",
   *           "jobscore": 500
   *         }
   *       ]
   *     },
   *     {
   *       "flowtime": <Last job's analysis_time>,
   *       "score": 700,
   *       "jobscores": [
   *         {
   *           "jobdefurl:" "url",
   *           "jobscore": 0
   *         },
   *         {
   *           "jobdefurl:" "url",
   *           "jobscore": 700
   *         }
   *       ]
   *     }
   *   ]
   * }
   * </pre>
   */
  public static Result restFlowGraphData(String flowUrl) {
    JsonArray datasets = new JsonArray();
    if (flowUrl == null || flowUrl.isEmpty()) {
      return ok(new Gson().toJson(datasets));
    }

    // Fetch available flow executions with latest JOB_HISTORY_LIMIT mr jobs.
    List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.FLOW_URL, flowUrl).order()
        .desc(JobResult.TABLE.ANALYSIS_TIME).setMaxRows(JOB_HISTORY_LIMIT).findList();
    if (results.size() == 0) {
      logger.info("No results for Job url");
    }
    Map<String, List<JobResult>> flowExecUrlToJobsMap =  limitHistoryResults(
        groupJobs(results, GroupBy.FLOW_EXECUTION_URL), results.size(), MAX_HISTORY_LIMIT);

    // Compute the graph data starting from the earliest available execution to latest
    List<String> keyList = new ArrayList<String>(flowExecUrlToJobsMap.keySet());
    for(int i = keyList.size() - 1; i >= 0; i--) {
      String flowExecUrl = keyList.get(i);
      int flowPerfScore = 0;
      JsonArray jobScores = new JsonArray();
      List<JobResult> mrJobsList = Lists.reverse(flowExecUrlToJobsMap.get(flowExecUrl));
      Map<String, List<JobResult>> jobDefUrlToJobsMap = groupJobs(mrJobsList, GroupBy.JOB_DEFINITION_URL);

      // Compute the execution records
      for (String jobDefUrl : jobDefUrlToJobsMap.keySet()) {
        // Compute job perf score
        int jobPerfScore = 0;
        for (JobResult job : jobDefUrlToJobsMap.get(jobDefUrl)) {
          jobPerfScore += getMRJobScore(job);
        }

        // A job in jobscores list
        JsonObject jobScore = new JsonObject();
        jobScore.addProperty("jobscore", jobPerfScore);
        jobScore.addProperty("jobdefurl", jobDefUrl);

        jobScores.add(jobScore);
        flowPerfScore += jobPerfScore;
      }

      // Execution record
      JsonObject dataset = new JsonObject();
      dataset.addProperty("flowtime", mrJobsList.get(mrJobsList.size() - 1).analysisTime);
      dataset.addProperty("score", flowPerfScore);
      dataset.add("jobscores", jobScores);

      datasets.add(dataset);
    }

    return ok(new Gson().toJson(datasets));
  }

  /**
   * The data for plotting the job history graph. While plotting the job history
   * graph an ajax call is made to this to fetch the graph data.
   *
   * Data Returned:
   * <pre>
   * {@code
   *   [
   *     {
   *       "flowtime": <Last job's analysis_time>,
   *       "score": 1000,
   *       "stagescores": [
   *         {
   *           "stageid:" "id",
   *           "stagescore": 500
   *         },
   *         {
   *           "stageid:" "id",
   *           "stagescore": 500
   *         }
   *       ]
   *     },
   *     {
   *       "flowtime": <Last job's analysis_time>,
   *       "score": 700,
   *       "stagescores": [
   *         {
   *           "stageid:" "id",
   *           "stagescore": 0
   *         },
   *         {
   *           "stageid:" "id",
   *           "stagescore": 700
   *         }
   *       ]
   *     }
   *   ]
   * }
   * </pre>
   */
  public static Result restJobGraphData(String jobUrl) {
    JsonArray datasets = new JsonArray();
    if (jobUrl == null || jobUrl.isEmpty()) {
      return ok(new Gson().toJson(datasets));
    }

    // Fetch available flow executions with latest JOB_HISTORY_LIMIT mr jobs.
    List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.JOB_URL, jobUrl).order()
        .desc(JobResult.TABLE.ANALYSIS_TIME).setMaxRows(JOB_HISTORY_LIMIT).findList();
    if (results.size() == 0) {
      logger.info("No results for Job url");
    }
    Map<String, List<JobResult>> flowExecUrlToJobsMap =  limitHistoryResults(
        groupJobs(results, GroupBy.FLOW_EXECUTION_URL), results.size(), MAX_HISTORY_LIMIT);

    // Compute the graph data starting from the earliest available execution to latest
    List<String> keyList = new ArrayList<String>(flowExecUrlToJobsMap.keySet());
    for(int i = keyList.size() - 1; i >= 0; i--) {
      String flowExecUrl = keyList.get(i);
      int jobPerfScore = 0;
      JsonArray stageScores = new JsonArray();
      List<JobResult> mrJobsList = Lists.reverse(flowExecUrlToJobsMap.get(flowExecUrl));
      for (JobResult job : flowExecUrlToJobsMap.get(flowExecUrl)) {

        // Each MR job triggered by jobUrl for flowExecUrl
        int mrPerfScore = 0;
        for (JobHeuristicResult heuristicResult : job.heuristicResults) {
          mrPerfScore += getHeuristicScore(heuristicResult);
        }

        // A particular mr stage
        JsonObject stageScore = new JsonObject();
        stageScore.addProperty("stageid", job.jobId);
        stageScore.addProperty("stagescore", mrPerfScore);

        stageScores.add(stageScore);
        jobPerfScore += mrPerfScore;
      }

      // Execution record
      JsonObject dataset = new JsonObject();
      dataset.addProperty("flowtime", mrJobsList.get(mrJobsList.size() - 1).analysisTime);
      dataset.addProperty("score", jobPerfScore);
      dataset.add("stagescores", stageScores);

      datasets.add(dataset);
    }

    return ok(new Gson().toJson(datasets));
  }

  /**
   * Calculates and returns the Heuristic Score for MapReduce Jobs.
   *
   * Heuristic Score = Number of Tasks(map/reduce) * Severity (When severity > 1)
   *
   * @param heuristicResult The Heuristic whose score has to be computed
   * @return The Score
   */
  private static int getHeuristicScore(JobHeuristicResult heuristicResult) {
    int heuristicScore = 0;

    int severity = heuristicResult.severity.getValue();
    if (severity != 0 && severity != 1) {
      for (String[] dataArray : heuristicResult.getDataArray()) {
        if (dataArray[0] != null && dataArray[0].toLowerCase().equals("number of tasks")) {
          return severity * Integer.parseInt(dataArray[1]);
        }
      }
    }

    return heuristicScore;
  }

  /**
   * Calculates and return the Mapreduce job score.
   *
   * Job Score = Sum of individual Heuristic Scores
   *
   * @param job The JobResult whose score has to be computed
   * @return The Score
   */
  private static int getMRJobScore(JobResult job) {
    int jobScore = 0;

    for (JobHeuristicResult heuristicResult : job.heuristicResults) {
      jobScore += getHeuristicScore(heuristicResult);
    }

    return jobScore;
  }

  public static Result testEmail() {

    DynamicForm form = Form.form().bindFromRequest(request());
    String jobId = form.get(JOB_ID);
    if (jobId != null && !jobId.isEmpty()) {
      JobResult result = JobResult.find.byId(jobId);
      if (result != null) {
        return ok(emailcritical.render(result));
      }
    }
    return notFound();
  }

}