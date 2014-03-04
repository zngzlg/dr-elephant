package com.linkedin.drelephant.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HadoopTaskData {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");
    private HadoopCounterHolder counterHolder;
    private long startTime;
    private long endTime;
    private long shuffleTime = 0;
    private long sortTime = 0;
    private String taskDetailPage;

    private TaskID taskId;

    public HadoopTaskData(RunningJob job, TaskReport task, boolean isReducer) {
        this(new HadoopCounterHolder(task.getCounters()), task.getStartTime(), task.getFinishTime(), task.getTaskID());
        taskDetailPage = getTaskDetailsPage(job, task.getTaskID());
    }

    public HadoopTaskData(HadoopCounterHolder counterHolder, long startTime, long endTime, TaskID taskId) {
        this.counterHolder = counterHolder;
        this.startTime = startTime;
        this.endTime = endTime;
        this.taskId = taskId;
        taskDetailPage = null;
    }

    public HadoopCounterHolder getCounters() {
        return counterHolder;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getRunTime() {
        return endTime - startTime;
    }

    public long getExecutionTime() {
        return endTime - startTime - shuffleTime - sortTime;
    }

    public long getShuffleTime() {
        return shuffleTime;
    }

    public long getSortTime() {
        return sortTime;
    }

    public void setShuffleTime(long shuffleTime) {
        this.shuffleTime = shuffleTime;
    }

    public void setSortTime(long sortTime) {
        this.sortTime = sortTime;
    }

    public TaskID getTaskId() {
        return taskId;
    }

    public void fetchTaskDetails() {
        if (taskDetailPage == null) {
            return;
        }
        try {
            URL url = new URL(taskDetailPage);
            AuthenticatedURL.Token token = new AuthenticatedURL.Token();
            HttpURLConnection conn = new AuthenticatedURL().openConnection(url, token);
            String data = IOUtils.toString(conn.getInputStream());
            Document doc = Jsoup.parse(data);
            Elements rows = doc.select("table").select("tr");
            for (int i = 1; i < rows.size(); i++) {
                Element row = rows.get(i);
                Elements cells = row.select("> td");
                if (cells.size() < 12) {
                    continue;
                }
                boolean succeeded = cells.get(2).html().trim().equals("SUCCEEDED");
                if (succeeded) {
                    try {
                        String startTime = cells.get(4).html().trim();
                        String shuffleTime = cells.get(5).html().trim();
                        String sortTime = cells.get(6).html().trim();
                        if (shuffleTime.contains("(")) {
                            shuffleTime = shuffleTime.substring(0, shuffleTime.indexOf("(") - 1);
                        }
                        if (sortTime.contains("(")) {
                            sortTime = sortTime.substring(0, sortTime.indexOf("(") - 1);
                        }
                        long start = dateFormat.parse(startTime).getTime();
                        long shuffle = dateFormat.parse(shuffleTime).getTime();
                        long sort = dateFormat.parse(sortTime).getTime();
                        this.shuffleTime = (shuffle - start);
                        this.sortTime = (sort - shuffle);
                    }
                    catch (ParseException e) {
                        //Ignored //e.printStackTrace();
                    }
                }
            }
        }
        catch (MalformedURLException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (AuthenticationException e) {
            e.printStackTrace();
        }
    }

    private String getTaskDetailsPage(RunningJob job, TaskID taskId) {
        String jobDetails = job.getTrackingURL();
        String root = jobDetails.substring(0, jobDetails.indexOf("jobdetails.jsp"));
        return root + "taskdetails.jsp?tipid=" + taskId.toString();
    }
}
