package controllers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.persistence.Entity;

import model.JobResult;
import model.StringResult;
import views.html.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import play.api.templates.Html;
import play.data.DynamicForm;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.ExpressionList;
import com.avaje.ebean.RawSql;
import com.avaje.ebean.RawSqlBuilder;
import com.avaje.ebean.annotation.Sql;
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

  private static final RawSql distinctExecs = RawSqlBuilder
      .parse("select distinct job_exec_url from job_result")
      .columnMapping("job_exec_url", "string").create();

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
    long now = System.currentTimeMillis();

    String jobUrl = request().queryString().get("job")[0];

    // Find all MR job executions that are from the same logical job
    List<StringResult> urls =
        Ebean.find(StringResult.class).setRawSql(distinctExecs).where()
        .eq("job_url", jobUrl).setMaxRows(10).findList();

    List<Pair<String, List<JobResult>>> results = relatedJobs(urls);

    return ok(related.render(jobUrl, results));
  }

  /**
   * A listing of all other jobs that were found from the same flow execution.
   */
  public static Result flowRelated() {
    long now = System.currentTimeMillis();

    String execurl = request().queryString().get("flowexec")[0];

    // Find all job executions that were part of the given flow execution
    List<StringResult> urls =
        Ebean.find(StringResult.class).setRawSql(distinctExecs).where()
        .eq("flow_exec_url", execurl).findList();

    // For each of the above jobs, find which MR jobs it spawned
    List<Pair<String, List<JobResult>>> results = relatedJobs(urls);

    return ok(related.render(execurl, results));
  }

  private static List<Pair<String, List<JobResult>>> relatedJobs(
      List<StringResult> urls) {
    List<Pair<String, List<JobResult>>> results =
        new ArrayList<Pair<String, List<JobResult>>>();
    for (StringResult result : urls) {
      String url = result.getString();
      List<JobResult> similar =
          JobResult.find.fetch("heuristicResults").where()
          .eq("job_exec_url", url).setMaxRows(50).findList();
      results.add(new ImmutablePair<String, List<JobResult>>(url, similar));
    }
    return results;
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
