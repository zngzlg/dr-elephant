package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElephantRunner implements Runnable {
    private static final long WAIT_INTERVAL = 30 * 1000;
    private static final Logger logger = Logger.getLogger(ElephantRunner.class);
    private AtomicBoolean running = new AtomicBoolean(true);
    private Set<JobID> previousJobs = new HashSet<JobID>();
    private File storage;

    public ElephantRunner(File storage) {
        this.storage = storage;

        Constants.load();
    }

    @Override
    public void run() {
        try {
            ElephantFetcher fetcher = new ElephantFetcher();
            ElephantAnalyser analyser = new ElephantAnalyser();
            long lastRun;

            while (running.get()) {
                lastRun = System.currentTimeMillis();

                logger.info("Fetching job list.");
                JobStatus[] jobs = fetcher.getJobList();
                Set<JobID> readyJobs = new HashSet<JobID>();
                for (JobStatus job : jobs) {
                    if (job.getRunState() == JobStatus.SUCCEEDED && job.isJobComplete()) {
                        readyJobs.add(job.getJobID());
                    }
                }

                logger.info("Cleaning up previous runs.");
                //Cleanup jobs no longer tracked by jobtracker
                Set<JobID> intersection = new HashSet<JobID>();
                for (JobID jobId : readyJobs) {
                    //Job previously analysed, still tracked by jobtracker
                    if (previousJobs.contains(jobId)) {
                        intersection.add(jobId);
                    }
                    //Otherwise it is no longer tracked, so we can safely dump it.
                }

                previousJobs = intersection;

                //Remove previously analysed jobs
                readyJobs.removeAll(previousJobs);

                logger.info(readyJobs.size() + " jobs to analyse.");

                //Analyse all ready jobs
                for (JobID jobId : readyJobs) {
                    try {
                        logger.info("Looking at job " + jobId);
                        HadoopJobData jobData = fetcher.getJobData(jobId);
                        if (jobData == null) {
                            continue;
                        }
                        logger.info("Analysing");
                        HeuristicResult analysisResult = analyser.analyse(jobData);

                        logger.info(analysisResult.getMessage());

                        //Write data to disk
                        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(storage, true)));
                        out.println("Job " + jobId);
                        out.println(analysisResult.getMessage());
                        out.println();
                        out.close();

                        previousJobs.add(jobId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                logger.info("Went over all jobs. Waiting for refresh.");
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void kill() {
        running.set(false);
    }
}
