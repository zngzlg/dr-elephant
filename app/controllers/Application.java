package controllers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import model.JobHeuristicResult;
import model.JobResult;
import model.JobType;

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
import views.html.help;
import views.html.index;
import views.html.multijob;
import views.html.relatedjob;
import views.html.search;
import views.html.singlejob;

import com.avaje.ebean.ExpressionList;
import com.avaje.ebean.RawSql;
import com.avaje.ebean.RawSqlBuilder;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.util.HeuristicConf;
import com.linkedin.drelephant.util.HeuristicConfData;


public class Application extends Controller {
  private static final Logger logger = Logger.getLogger(Application.class);
  private static final long DAY = 24 * 60 * 60 * 1000;
  private static final long FETCH_DELAY = 60 * 1000;
  private static final int PAGE_LENGTH = 25;
  private static final int PAGE_BAR_LENGTH = 10;
  private static final String FORM_JOB_ID = "jobid";
  private static final String FORM_FLOW_URL = "flowurl";
  private static final String FORM_USER = "user";
  private static final String FORM_SEVERITY = "severity";
  private static final String FORM_JOB_TYPE = "jobtype";
  private static final String FORM_ANALYSIS = "analysis";
  private static final String FORM_START_DATE = "start-date";
  private static final String FORM_END_DATE = "end-date";
  private static long _lastFetch = 0;
  private static int _numJobsAnalyzed = 0;
  private static int _numJobsCritical = 0;
  private static int _numJobsSevere = 0;
  private static Map<String, Html> _helpPages = new HashMap<String, Html>();
  private static PaginationStats paginationStats = new PaginationStats(PAGE_LENGTH, PAGE_BAR_LENGTH);

  static {
    try {
      logger.info("Loading pluggable heuristics help pages.");
      fillHelpPages();
    } catch (Exception e) {
      logger.error("Error loading pluggable heuristics help pages.", e);
      throw new RuntimeException(e);
    }
  }

  public static StringBuilder getSqlJoinQuery() {
    final StringBuilder sqlJoinQueryBuilder = new StringBuilder();
    sqlJoinQueryBuilder.append("SELECT " + JobResult.getColumnList());
    sqlJoinQueryBuilder.append(" FROM " + JobResult.TABLE.TABLE_NAME + " JOIN " + JobHeuristicResult.TABLE.TABLE_NAME);
    sqlJoinQueryBuilder.append(" ON " + JobHeuristicResult.TABLE.TABLE_NAME + "." + JobHeuristicResult.TABLE.JOB_JOB_ID
        + " = " + JobResult.TABLE.TABLE_NAME + "." + JobResult.TABLE.JOB_ID);
    return sqlJoinQueryBuilder;
  }

  public static Result search() {
    // Search and display job information when job id or flow execution url is provided.
    DynamicForm form = Form.form().bindFromRequest(request());
    String jobId = form.get(FORM_JOB_ID);
    jobId = jobId != null ? jobId.trim() : "";
    String flowUrl = form.get(FORM_FLOW_URL);
    flowUrl = (flowUrl != null) ? flowUrl.trim() : null;
    if (!jobId.isEmpty()) {
      JobResult result = JobResult.find.byId(jobId);
      if (result != null) {
        return ok(search.render(null, singlejob.render(result)));
      } else {
        return ok(search.render(null, singlejob.render(null)));
      }
    } else if (flowUrl != null && !flowUrl.isEmpty()) {
      List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.FLOW_EXEC_URL, flowUrl).findList();
      Map<String, List<JobResult>> map = groupJobsByExec(results);
      return ok(search.render(null, relatedjob.render(flowUrl, map)));
    }

    // Paginate the results
    int pageLength = paginationStats.getPageLength();
    paginationStats.setCurrentPage(1);
    final Map<String, String[]> searchString = request().queryString();
    if (searchString.containsKey("page")) {
      try {
        paginationStats.setCurrentPage(Integer.parseInt(searchString.get("page")[0]));
      } catch (NumberFormatException ex) {
        logger.error("Error parsing page number. Setting current page to 1.");
        paginationStats.setCurrentPage(1);
      }
    }
    int currentPage = paginationStats.getCurrentPage();
    int paginationBarStartIndex = paginationStats.getPaginationBarStartIndex();
    ExpressionList<JobResult> query = generateQuery();
    List<JobResult> results =
        query.order().desc("analysisTime").fetch("heuristicResults")
            .setFirstRow((paginationBarStartIndex - 1) * pageLength)
            .setMaxRows((paginationStats.getPageBarLength() - 1) * pageLength + 1).findList();
    paginationStats.setQueryString(getQueryString());
    if (results.isEmpty() || currentPage > paginationStats.computePaginationBarEndIndex(results.size())) {
      return ok(search.render(null, singlejob.render(null)));
    } else {
      return ok(search.render(
          paginationStats,
          multijob.render(
              "Results",
              results.subList((currentPage - paginationBarStartIndex) * pageLength,
                  Math.min(results.size(), (currentPage - paginationBarStartIndex + 1) * pageLength)))));
    }
  }

  private static String getQueryString() {
    List<BasicNameValuePair> fields = new LinkedList<BasicNameValuePair>();
    final Set<Map.Entry<String, String[]>> entries = request().queryString().entrySet();
    for (Map.Entry<String, String[]> entry : entries) {
      final String key = entry.getKey();
      final String value = entry.getValue()[0];
      if (!key.equals("page")) {
        fields.add(new BasicNameValuePair(key, value));
      }
    }
    if (fields.isEmpty()) {
      return null;
    } else {
      return URLEncodedUtils.format(fields, "utf-8");
    }
  }

  private static ExpressionList<JobResult> generateQuery() {
    SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
    DynamicForm form = Form.form().bindFromRequest(request());
    String username = form.get(FORM_USER);
    username = username != null ? username.trim().toLowerCase() : null;
    String severity = form.get(FORM_SEVERITY);
    String jobType = form.get(FORM_JOB_TYPE);
    String analysis = form.get(FORM_ANALYSIS);
    String dateStart = form.get(FORM_START_DATE);
    String dateEnd = form.get(FORM_END_DATE);

    ExpressionList<JobResult> query = JobResult.find.where();

    if (username != null && !username.isEmpty()) {
      query = query.like(JobResult.TABLE.USERNAME, username);
    }
    if (jobType != null && !jobType.isEmpty()) {
      query = query.eq(JobResult.TABLE.JOB_TYPE, JobType.getDbName(jobType));
    }
    if (severity != null && !severity.isEmpty()) {
      if (analysis == null || analysis.isEmpty()) {
        query = query.ge(JobResult.TABLE.SEVERITY, severity);
      } else {
        RawSql rawsql = RawSqlBuilder.parse(getSqlJoinQuery().toString()).create();
        query =
            query.query().setRawSql(rawsql).where()
                .eq(JobHeuristicResult.TABLE.TABLE_NAME + "." + JobHeuristicResult.TABLE.ANALYSIS_NAME, analysis)
                .ge(JobHeuristicResult.TABLE.TABLE_NAME + "." + JobHeuristicResult.TABLE.SEVERITY, severity);
      }
    }
    if (dateStart != null && !dateStart.isEmpty()) {
      try {
        Date date = dateFormat.parse(dateStart);
        query = query.gt(JobResult.TABLE.START_TIME, date.getTime());
      } catch (ParseException e) {
        logger.error("Error while parsing dateStart", e);
        e.printStackTrace();
      }
    }
    if (dateEnd != null && !dateEnd.isEmpty()) {
      try {
        Date date = dateFormat.parse(dateEnd);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, 1);
        date = c.getTime();
        query = query.lt(JobResult.TABLE.START_TIME, date.getTime());
      } catch (ParseException e) {
        logger.error("Error while parsing dateEnd", e);
        e.printStackTrace();
      }
    }
    return query;
  }

  public static Result dashboard(int page) {
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

    return ok(index.render(_numJobsAnalyzed, _numJobsSevere, _numJobsCritical,
        multijob.render("Latest analysis", results)));
  }

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
    return ok(help.render(title, page));
  }

  //create a map to cache pages.
  private static void fillHelpPages() {
    logger.info("Loading help pages for pluggable heuristics");
    HeuristicConf conf = HeuristicConf.instance();
    List<HeuristicConfData> heuristicsConfList = conf.getHeuristicsConfData();
    for (HeuristicConfData heuristicConf : heuristicsConfList) {
      Class<?> viewClass = null;
      String heuristicName = null;
      try {
        String viewName = heuristicConf.getViewName();
        logger.info("Loading help page " + viewName);
        viewClass = Play.current().classloader().loadClass(viewName);
        Class<?> heuristicClass = Play.current().classloader().loadClass(heuristicConf.getClassName());
        heuristicName = (String) heuristicClass.getDeclaredField("HEURISTIC_NAME").get(null);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not find class " + heuristicConf.getViewName(), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("field HEURISTIC_NAME in class " + heuristicConf.getClassName()
            + " is not accessible.");
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("No field HEURISTIC_NAME in class " + heuristicConf.getClassName());
      } catch (Exception e) {
        // More descriptive on other Runtime Exceptions such as NullPointerException IllegalArgumentException
        throw new RuntimeException("No valid field HEURISTIC_NAME in class" + heuristicConf.getClassName());
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
   * A listing of all MR jobs from historic executions of the same job
   */
  public static Result allJobExecs() {

    String jobUrl = request().queryString().get("job")[0];
    List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.JOB_URL, jobUrl).findList();

    if (results.size() == 0) {
      return notFound("Unable to find record on job definition url: " + jobUrl);
    }

    Map<String, List<JobResult>> map = groupJobsByExec(results);
    return ok(search.render(null, relatedjob.render(jobUrl, map)));
  }

  /**
   * A listing of all other jobs that were found from the same flow execution.
   */
  public static Result flowRelated() {

    String execUrl = request().queryString().get("flowexec")[0];
    List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.FLOW_EXEC_URL, execUrl).findList();

    if (results.size() == 0) {
      return notFound("Unable to find record on flow exec: " + execUrl);
    }

    Map<String, List<JobResult>> map = groupJobsByExec(results);
    return ok(search.render(null, relatedjob.render(execUrl, map)));
  }

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

  public static Result restFlowExecResult(String flowExecUrl) {

    if (flowExecUrl == null || flowExecUrl.isEmpty()) {
      return badRequest("No flow exec url provided.");
    }

    List<JobResult> results = JobResult.find.where().eq(JobResult.TABLE.FLOW_EXEC_URL, flowExecUrl).findList();

    if (results.size() == 0) {
      return notFound("Unable to find record on flow exec url: " + flowExecUrl);
    }

    Map<String, List<JobResult>> resMap = groupJobsByExec(results);

    return ok(Json.toJson(resMap));
  }

  private static Map<String, List<JobResult>> groupJobsByExec(List<JobResult> results) {

    Map<String, List<JobResult>> resultMap = new HashMap<String, List<JobResult>>();

    for (JobResult result : results) {
      String field = result.jobExecUrl;
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

  public static Result testEmail() {

    DynamicForm form = Form.form().bindFromRequest(request());
    String jobId = form.get("jobid");
    if (jobId != null && !jobId.isEmpty()) {
      JobResult result = JobResult.find.byId(jobId);
      if (result != null) {
        return ok(emailcritical.render(result));
      }
    }
    return notFound();
  }
}
