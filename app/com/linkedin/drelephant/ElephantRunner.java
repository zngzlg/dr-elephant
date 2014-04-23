package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopSecurity;
import com.linkedin.drelephant.notifications.EmailThread;
import com.linkedin.drelephant.util.Utils;
import model.JobHeuristicResult;
import model.JobResult;
import model.JobType;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.log4j.Logger;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElephantRunner implements Runnable {
    private static final long WAIT_INTERVAL = 5 * 60 * 1000;
    private static final Logger logger = Logger.getLogger(ElephantRunner.class);
    private AtomicBoolean running = new AtomicBoolean(true);
    private EmailThread emailer = new EmailThread();
    private boolean firstRun = true;
    private HadoopSecurity hadoopSecurity;

    @Override
    public void run() {
        hadoopSecurity = new HadoopSecurity();
        hadoopSecurity.doAs(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                Constants.load();
                emailer.start();
                Set<JobID> previousJobs = new HashSet<JobID>();
                long lastRun;
                ElephantFetcher fetcher;
                try {
                    fetcher = new ElephantFetcher();
                } catch (Exception e) {
                    logger.error("Error initializing fetcher", e);
                    return null;
                }

                while (running.get()) {
                    lastRun = System.currentTimeMillis();

                    try {
                        logger.info("Fetching job list.");
                        hadoopSecurity.checkLogin();
                        JobStatus[] jobs = fetcher.getJobList();
                        if (jobs == null) {
                            throw new IllegalArgumentException("Jobtracker returned 'null' for job list");
                        }

                        Set<JobID> successJobs = filterSuccessfulJobs(jobs);
                        successJobs = filterPreviousJobs(successJobs, previousJobs);

                        logger.info(successJobs.size() + " jobs to analyse.");

                        //Analyse all ready jobs
                        for (JobID jobId : successJobs) {
                            boolean success = analyzeJob(fetcher, jobId);
                            if (success) {
                                previousJobs.add(jobId);
                            }
                        }
                        logger.info("Finished all jobs. Waiting for refresh.");

                    } catch (Exception e) {
                        logger.error("Error in Runner thread", e);
                    }

                    //Wait for long enough
                    long nextRun = lastRun + WAIT_INTERVAL;
                    long waitTime = nextRun - System.currentTimeMillis();
                    while (running.get() && waitTime > 0) {
                        try {
                            Thread.sleep(waitTime);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        waitTime = nextRun - System.currentTimeMillis();
                    }
                }
                return null;
            }
        });
    }

    private Set<JobID> filterSuccessfulJobs(JobStatus[] jobs) {
        Set<JobID> successJobs = new HashSet<JobID>();
        for (JobStatus job : jobs) {
            if (job.getRunState() == JobStatus.SUCCEEDED && job.isJobComplete()) {
                successJobs.add(job.getJobID());
            }
        }
        return successJobs;
    }

    private Set<JobID> filterPreviousJobs(Set<JobID> jobs, Set<JobID> previousJobs) {
        logger.info("Cleaning up previous runs.");
        //On first run, check against DB
        if (firstRun) {
            Set<JobID> newJobs = new HashSet<JobID>();
            for (JobID jobId : jobs) {
                JobResult prevResult = JobResult.find.byId(jobId.toString());
                if (prevResult == null) {
                    //Job not found, add to new jobs list
                    newJobs.add(jobId);
                } else {
                    //Job found, add to old jobs list
                    previousJobs.add(jobId);
                }
            }
            jobs = newJobs;
            firstRun = false;
        } else {
            //Remove untracked jobs
            previousJobs.retainAll(jobs);
            //Remove previously analysed jobs
            jobs.removeAll(previousJobs);
        }

        return jobs;
    }

    /**
     * @param fetcher
     * @param jobId
     * @return true if analysis succeeded
     */
    private boolean analyzeJob(ElephantFetcher fetcher, JobID jobId) {
        ElephantAnalyser analyser = ElephantAnalyser.instance();
        try {
            logger.info("Looking at job " + jobId);
            HadoopJobData jobData = fetcher.getJobData(jobId);

            //Job wiped from jobtracker already.
            if (jobData == null) {
                return true;
            }

            HeuristicResult[] analysisResults = analyser.analyse(jobData);
            JobType jobType = analyser.getJobType(jobData);

            //Save to DB
            JobResult result = new JobResult();
            result.job_id = jobId.toString();
            result.url = jobData.getUrl();
            result.username = jobData.getUsername();
            result.startTime = jobData.getStartTime();
            result.analysisTime = System.currentTimeMillis();
            result.jobName = jobData.getJobName();
            result.jobType = jobType;

            //Truncate long names
            if (result.jobName.length() > 100) {
                result.jobName = result.jobName.substring(0, 97) + "...";
            }
            result.heuristicResults = new ArrayList<JobHeuristicResult>();

            Severity worstSeverity = Severity.NONE;

            for (HeuristicResult heuristicResult : analysisResults) {
                JobHeuristicResult detail = new JobHeuristicResult();
                detail.analysisName = heuristicResult.getAnalysis();
                detail.data = heuristicResult.getDetailsCSV();
                detail.dataColumns = heuristicResult.getDetailsColumns();
                detail.severity = heuristicResult.getSeverity();
                if (detail.dataColumns < 1) {
                    detail.dataColumns = 1;
                }
                result.heuristicResults.add(detail);
                worstSeverity = Severity.max(worstSeverity, detail.severity);
            }

            result.severity = worstSeverity;

            Map<String, String> metaUrls = analyser.getMetaUrls(jobData);
            String[] csvLines = new String[metaUrls.size()];
            int i = 0;
            for (Map.Entry<String, String> entry : metaUrls.entrySet()) {
                csvLines[i] = Utils.createCsvLine(entry.getKey(), entry.getValue());
                i++;
            }
            result.metaUrls = Utils.combineCsvLines(csvLines);

            result.save();


            emailer.enqueue(result);
            return true;
        } catch (Exception e) {
            logger.error("Error analysing job", e);
        }
        return false;
    }

    public void kill() {
        running.set(false);
        emailer.kill();
    }
}
