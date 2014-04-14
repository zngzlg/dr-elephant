package controllers;

import com.avaje.ebean.ExpressionList;
import com.linkedin.drelephant.ElephantAnalyser;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.analysis.heuristics.*;
import model.JobResult;
import play.Logger;
import play.api.templates.Html;
import play.data.DynamicForm;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

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
            if (severity != null && !severity.isEmpty()) {
                query = query.ge("heuristicResults.severity", severity);
            }
            if (analysis != null && !analysis.isEmpty()) {
                query = query.eq("heuristicResults.analysisName", analysis);
            }
            if (dateStart != null && !dateStart.isEmpty()) {
                try {
                    Date date = dateFormat.parse(dateStart);
                    Logger.debug(date.toString());
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
            List<JobResult> results = query
                    .order().desc("analysisTime")
                    .setMaxRows(50)
                    .fetch("heuristicResults")
                    .findList();
            return ok(search.render(multijob.render("Results", results)));
        }
    }

    public static Result dashboard(int page) {
        long now = System.currentTimeMillis();
        if (now - lastFetch > FETCH_DELAY) {
            numJobsAnalyzed = JobResult.find.where()
                    .gt("analysisTime", now - DAY)
                    .findRowCount();
            numJobsCritical = JobResult.find.where()
                    .gt("analysisTime", now - DAY)
                    .eq("severity", Severity.CRITICAL.getValue())
                    .findRowCount();
            numJobsSevere = JobResult.find.where()
                    .gt("analysisTime", now - DAY)
                    .eq("severity", Severity.SEVERE.getValue())
                    .findRowCount();
            lastFetch = now;
        }
        List<JobResult> results = JobResult.find.where()
                .gt("analysisTime", now - DAY)
                .order().desc("analysisTime")
                .setMaxRows(50)
                .fetch("heuristicResults")
                .findList();

        return ok(index.render(numJobsAnalyzed, numJobsSevere, numJobsCritical, multijob.render("Latest analysis", results)));
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
