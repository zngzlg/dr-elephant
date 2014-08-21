package controllers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import model.JobResult;
import views.html.*;

import play.api.templates.Html;
import play.data.DynamicForm;
import play.data.Form;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import com.avaje.ebean.ExpressionList;
import com.linkedin.drelephant.ElephantAnalyser;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.analysis.heuristics.MapperDataSkewHeuristic;
import com.linkedin.drelephant.analysis.heuristics.MapperInputSizeHeuristic;
import com.linkedin.drelephant.analysis.heuristics.MapperSpeedHeuristic;
import com.linkedin.drelephant.analysis.heuristics.ReducerDataSkewHeuristic;
import com.linkedin.drelephant.analysis.heuristics.ReducerTimeHeuristic;
import com.linkedin.drelephant.analysis.heuristics.ShuffleSortHeuristic;

public class Application extends Controller {
  private static final long DAY = 24 * 60 * 60 * 1000;
  private static final long FETCH_DELAY = 60 * 1000;
  private static long lastFetch = 0;
  private static int numJobsAnalyzed = 0;
  private static int numJobsCritical = 0;
  private static int numJobsSevere = 0;

  public static Result search() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String jobId = form.get("jobid");
    String username = form.get("user");
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
        query = query.eq("job_type", jobtype);
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
      List<JobResult> results =
          query.order().desc("analysisTime").setMaxRows(50)
          .fetch("heuristicResults").findList();
      return ok(search.render(multijob.render("Results", results)));
    }
  }

  public static Result dashboard(int page) {
    long now = System.currentTimeMillis();
    if (now - lastFetch > FETCH_DELAY) {
      numJobsAnalyzed =
          JobResult.find.where().gt("analysisTime", now - DAY).findRowCount();
      numJobsCritical =
          JobResult.find.where().gt("analysisTime", now - DAY)
          .eq("severity", Severity.CRITICAL.getValue()).findRowCount();
      numJobsSevere =
          JobResult.find.where().gt("analysisTime", now - DAY)
          .eq("severity", Severity.SEVERE.getValue()).findRowCount();
      lastFetch = now;
    }
    List<JobResult> results =
        JobResult.find.where().gt("analysisTime", now - DAY).order()
        .desc("analysisTime").setMaxRows(50).fetch("heuristicResults")
        .findList();

    return ok(index.render(numJobsAnalyzed, numJobsSevere, numJobsCritical,
        multijob.render("Latest analysis", results)));
  }

  public static Result help() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String topic = form.get("topic");

    Html page = null;
    String title = "Help";

    if (topic != null && !topic.isEmpty()) {
      if (topic.equals(MapperDataSkewHeuristic.heuristicName)) {
        page = helpMapperDataSkew.render();
      } else if (topic.equals(ReducerDataSkewHeuristic.heuristicName)) {
        page = helpReducerDataSkew.render();
      } else if (topic.equals(MapperInputSizeHeuristic.heuristicName)) {
        page = helpMapperInputSize.render();
      } else if (topic.equals(MapperSpeedHeuristic.heuristicName)) {
        page = helpMapperSpeed.render();
      } else if (topic.equals(ReducerTimeHeuristic.heuristicName)) {
        page = helpReducerTime.render();
      } else if (topic.equals(ShuffleSortHeuristic.heuristicName)) {
        page = helpShuffleSort.render();
      } else if (topic.equals(ElephantAnalyser.NO_DATA)) {
        page = helpNoData.render();
      }
      if (page != null) {
        title = topic;
      }
    }

    return ok(help.render(title, page));
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
