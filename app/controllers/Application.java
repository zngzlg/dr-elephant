package controllers;

import com.avaje.ebean.ExpressionList;
import com.linkedin.drelephant.analysis.HeuristicResult;
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
    private static int numJobsNonPerformant = 0;

    public static Result search() {
        return ok(search.render(null, HeuristicResult.possibleResults));
    }

    public static Result queryJob() {
        DynamicForm form = Form.form().bindFromRequest(request());
        String jobId = form.get("jobid");
        String username = form.get("username");
        String issue = form.get("issue");
        List<AnalysisResult> results = new ArrayList<AnalysisResult>();
        if (jobId != null) {
            AnalysisResult result = AnalysisResult.find.byId(jobId);
            if (result != null) {
                results.add(result);
            } else {
                //TODO: Show error
            }
        } else {
            ExpressionList<AnalysisResult> query = AnalysisResult.find.where();
            if (username != null) {
                query = query.eq("username", username);
            } else if (issue != null) {
                query = query.eq("message", issue);
            }
            List<AnalysisResult> result = query
                    .order().desc("analysisTime")
                    .setMaxRows(50)
                    .findList();
            results.addAll(result);
        }
        Collections.reverse(results);
        return ok(search.render(results, HeuristicResult.possibleResults));
    }

    public static Result dashboard(int page) {
        long now = System.currentTimeMillis();
        if (now - lastFetch > FETCH_DELAY) {
            numJobsAnalyzed = AnalysisResult.find.where()
                    .gt("analysisTime", now - DAY)
                    .findRowCount();
            numJobsNonPerformant = AnalysisResult.find.where()
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

        return ok(index.render(Integer.toString(numJobsAnalyzed), Integer.toString(numJobsNonPerformant), latestResults));
    }
}
