package com.linkedin.drelephant;

import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class ElephantFetcher {
    private static final Logger logger = Logger.getLogger(ElephantFetcher.class);

    private JobClient jobClient;

    public ElephantFetcher() throws IOException {
        init();
    }

    private void init() throws IOException {
        logger.info("Connecting to the jobtracker");
        Configuration config = new Configuration();
        jobClient = new JobClient(new JobConf(config));
    }

    public HadoopJobData getJobData(JobID job_id) throws IOException {
        RunningJob job = getJob(job_id);
        if (job == null) {
            return null;
        }

        JobStatus status = job.getJobStatus();
        String username = status.getUsername();
        long startTime = status.getStartTime();
        String jobUrl = job.getTrackingURL();
        String jobName = job.getJobName();

        TaskReport[] mapperTasks = getMapTaskReports(job_id);
        TaskReport[] reducerTasks = getReduceTaskReports(job_id);

        HadoopCounterHolder counterHolder = new HadoopCounterHolder(job.getCounters());
        HadoopTaskData[] mappers = new HadoopTaskData[mapperTasks.length];
        for (int i = 0; i < mapperTasks.length; i++) {
            mappers[i] = new HadoopTaskData(job, mapperTasks[i], false);
        }
        HadoopTaskData[] reducers = new HadoopTaskData[reducerTasks.length];
        for (int i = 0; i < reducerTasks.length; i++) {
            reducers[i] = new HadoopTaskData(job, reducerTasks[i], true);
        }

        Properties jobConf = null;
        try {
            jobConf = getJobConf(job);
        } catch (AuthenticationException e) {
            e.printStackTrace();
        }

        if (jobConf == null) {
            jobConf = new Properties();
        }

        return new HadoopJobData(counterHolder, mappers, reducers, jobConf)
                .setUsername(username).setStartTime(startTime).setUrl(jobUrl).setJobName(jobName);
    }

    private RunningJob getJob(JobID job_id) throws IOException {
        return jobClient.getJob(job_id);
    }

    private TaskReport[] getMapTaskReports(JobID job_id) throws IOException {
        return jobClient.getMapTaskReports(job_id);
    }

    private TaskReport[] getReduceTaskReports(JobID job_id) throws IOException {
        return jobClient.getReduceTaskReports(job_id);
    }

    public JobStatus[] getJobList() throws IOException {
        return jobClient.getAllJobs();
    }

    public Properties getJobConf(RunningJob job) throws IOException, AuthenticationException {
        Properties properties = new Properties();
        String jobconfUrl = getJobconfUrl(job);
        if (jobconfUrl == null) {
            return properties;
        }

        URL url = new URL(jobconfUrl);
        AuthenticatedURL.Token token = new AuthenticatedURL.Token();
        HttpURLConnection conn = new AuthenticatedURL().openConnection(url, token);
        String data = IOUtils.toString(conn.getInputStream());
        Document doc = Jsoup.parse(data);
        Elements rows = doc.select("table").select("tr");
        for (int i = 1; i < rows.size(); i++) {
            Element row = rows.get(i);
            Elements cells = row.select("> td");
            if (cells.size() == 2) {
                String key = cells.get(0).text().trim();
                String value = cells.get(1).text().trim();
                properties.put(key, value);
            }
        }
        return properties;
    }

    private String getJobconfUrl(RunningJob job) {
        String jobDetails = job.getTrackingURL();
        String root = jobDetails.substring(0, jobDetails.indexOf("jobdetails.jsp"));
        return root + "jobconf.jsp?jobid=" + job.getID().toString();
    }
}
