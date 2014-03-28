package controllers;

import com.avaje.ebean.ExpressionList;
import model.JobResult;
import play.data.DynamicForm;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.index;
import views.html.multijob;
import views.html.search;
import views.html.singlejob;

import java.util.List;

public class Application extends Controller {
    private static final long DAY = 24 * 60 * 60 * 1000;
    private static final long FETCH_DELAY = 60 * 1000;
    private static long lastFetch = 0;
    private static int numJobsAnalyzed = 0;

    public static Result search() {
        return ok(search.render(null));
    }

    public static Result queryJob() {
        DynamicForm form = Form.form().bindFromRequest(request());
        String jobId = form.get("jobid");
        String username = form.get("username");
        String severity = form.get("severity");
        if (jobId != null) {
            JobResult result = JobResult.find.byId(jobId);
            if (result != null) {
                return ok(search.render(singlejob.render(result)));
            } else {
                return ok(search.render(singlejob.render(null)));
            }
        } else {
            ExpressionList<JobResult> query = JobResult.find.where();
            if (username != null) {
                query = query.eq("username", username);
            } else if (severity != null) {
                query = query.ge("severity", severity);
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
            lastFetch = now;
        }
        List<JobResult> results = JobResult.find.where()
                .gt("analysisTime", now - DAY)
                .order().desc("analysisTime")
                .setMaxRows(50)
                .fetch("heuristicResults")
                .findList();

        return ok(index.render(Integer.toString(numJobsAnalyzed), multijob.render("Latest analysis", results)));
    }
}
