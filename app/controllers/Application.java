package controllers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import model.JobResult;
import model.JobType;
import views.html.*;
import play.api.Play;
import play.api.templates.Html;
import play.data.DynamicForm;
import play.data.Form;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import com.avaje.ebean.ExpressionList;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.util.HeuristicConf;
import com.linkedin.drelephant.util.HeuristicConfData;


public class Application extends Controller {
  private static final Logger logger = Logger.getLogger(Application.class);
  private static final long DAY = 24 * 60 * 60 * 1000;
  private static final long FETCH_DELAY = 60 * 1000;
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

  public static Result search() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String jobId = form.get("jobid");
    jobId = jobId != null ? jobId.trim() : null;
    String username = form.get("user");
    username = username != null ? username.trim() : null;
    String severity = form.get("severity");
    String jobtype = form.get("jobtype");
    String analysis = form.get("analysis");
    String dateStart = form.get("start-date");
    String dateEnd = form.get("end-date");
    SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
    if (jobId != null && !jobId.isEmpty()) {
      JobResult result = JobResult.find.byId(jobId);
      if (result != null) {
        return ok(search.render(singlejob.render(result)));
      } else {
        return ok(search.render(singlejob.render(null)));
      }
    } else {
      ExpressionList<JobResult> query = JobResult.find.where();
      if (username != null && !username.isEmpty()) {
        query = query.ilike("username", username);
      }
      if (jobtype != null && !jobtype.isEmpty()) {
        query = query.eq("job_type", JobType.getDbName(jobtype));
      }
      if (severity != null && !severity.isEmpty()) {
        query = query.ge("heuristicResults.severity", severity);
      }
      if (analysis != null && !analysis.isEmpty()) {
        query = query.eq("heuristicResults.analysisName", analysis);
      }
      if (dateStart != null && !dateStart.isEmpty()) {
        try {
          Date date = dateFormat.parse(dateStart);
          query = query.gt("startTime", date.getTime());
        } catch (ParseException e) {
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
          query = query.lt("startTime", date.getTime());
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
      List<JobResult> results = query.order().desc("analysisTime").setMaxRows(50).fetch("heuristicResults").findList();
      return ok(search.render(multijob.render("Results", results)));
    }
  }

  public static Result dashboard(int page) {
    long now = System.currentTimeMillis();
    if (now - _lastFetch > FETCH_DELAY) {
      _numJobsAnalyzed = JobResult.find.where().gt("analysisTime", now - DAY).findRowCount();
      _numJobsCritical =
          JobResult.find.where().gt("analysisTime", now - DAY).eq("severity", Severity.CRITICAL.getValue())
              .findRowCount();
      _numJobsSevere =
          JobResult.find.where().gt("analysisTime", now - DAY).eq("severity", Severity.SEVERE.getValue())
              .findRowCount();
      _lastFetch = now;
    }
    List<JobResult> results =
        JobResult.find.where().gt("analysisTime", now - DAY).order().desc("analysisTime").setMaxRows(50)
            .fetch("heuristicResults").findList();

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
    List<JobResult> results = JobResult.find.where().eq("job_url", jobUrl).findList();

    if (results.size() == 0) {
      return notFound("Unable to find record on job definition url: " + jobUrl);
    }

    Map<String, List<JobResult>> map = groupJobsByExec(results);
    return ok(related.render(jobUrl, map));
  }

  /**
   * A listing of all other jobs that were found from the same flow execution.
   */
  public static Result flowRelated() {

    String execUrl = request().queryString().get("flowexec")[0];
    List<JobResult> results = JobResult.find.where().eq("flow_exec_url", execUrl).findList();

    if (results.size() == 0) {
      return notFound("Unable to find record on flow exec: " + execUrl);
    }

    Map<String, List<JobResult>> map = groupJobsByExec(results);
    return ok(related.render(execUrl, map));
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

    List<JobResult> result = JobResult.find.where().eq("job_exec_url", jobExecUrl).findList();

    if (result.size() == 0) {
      return notFound("Unable to find record on job exec url: " + jobExecUrl);
    }

    return ok(Json.toJson(result));
  }

  public static Result restFlowExecResult(String flowExecUrl) {

    if (flowExecUrl == null || flowExecUrl.isEmpty()) {
      return badRequest("No flow exec url provided.");
    }

    List<JobResult> results = JobResult.find.where().eq("flow_exec_url", flowExecUrl).findList();

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
