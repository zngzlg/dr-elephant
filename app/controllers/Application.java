package controllers;

import model.AnalysisResult;
import play.data.DynamicForm;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.index;
import views.html.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Application extends Controller {
    private static final long DAY = 24 * 60 * 60 * 1000;
    private static final long FETCH_DELAY = 60 * 1000;
    private static long lastFetch = 0;
    private static int numJobsAnalyzed = 0;
    private static int numJobsNonPerforate = 0;

    public static Result search() {
        return ok(search.render(null));
    }

    public static Result queryJob() {
        DynamicForm form = Form.form().bindFromRequest(request());
        String jobId = form.get("jobid");
        String username = form.get("username");
        List<AnalysisResult> results = new ArrayList<AnalysisResult>();
        if (jobId != null) {
            AnalysisResult result = AnalysisResult.find.byId(jobId);
            if (result != null) {
                results.add(result);
            } else {
                //TODO: Show error
            }
        } else if (username != null) {
            results.addAll(AnalysisResult.find.where().eq("username", username).findList());
        }
        Collections.reverse(results);
        return ok(search.render(results));
    }

    public static Result dashboard(int page) {
        long now = System.currentTimeMillis();
        if (now - lastFetch > FETCH_DELAY) {
            numJobsAnalyzed = AnalysisResult.find.where()
                    .gt("analysisTime", now - DAY)
                    .findRowCount();
            numJobsNonPerforate = AnalysisResult.find.where()
                    .gt("analysisTime", now - DAY)
                    .eq("success", false)
                    .findRowCount();
            lastFetch = now;
        }
        List<AnalysisResult> latestResults = AnalysisResult.find.where()
                .gt("analysisTime", now - DAY)
                //.eq("success", false)
                .order().desc("analysisTime")
                .setMaxRows(50)
                .findList();

        return ok(index.render(Integer.toString(numJobsAnalyzed), Integer.toString(numJobsNonPerforate), latestResults));
    }
}
