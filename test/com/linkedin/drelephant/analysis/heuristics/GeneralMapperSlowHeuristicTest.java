package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import junit.framework.TestCase;
import org.apache.hadoop.mapred.TaskID;

public class GeneralMapperSlowHeuristicTest extends TestCase {
    Heuristic heuristic = new GeneralMapperSlowHeuristic();

    public void testApplyFail() throws Exception {
        HadoopCounterHolder counters = new HadoopCounterHolder();
        HadoopTaskData[] mappers = new HadoopTaskData[3];
        HadoopTaskData[] reducers = new HadoopTaskData[3];

        HadoopCounterHolder counter_a = new HadoopCounterHolder();
        HadoopCounterHolder counter_b = new HadoopCounterHolder();
        HadoopCounterHolder counter_c = new HadoopCounterHolder();

        //2MB/s
        long runtime = 20 * 60; // 20 minutes

        counter_a.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 2 * 1024 * 1024);
        counter_b.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 2 * 1024 * 1024);
        counter_c.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 2 * 1024 * 1024);

        mappers[0] = new HadoopTaskData(counter_a, 0, runtime * 1000, TaskID.forName("task_201402260232_111111_r_000000"));
        mappers[1] = new HadoopTaskData(counter_b, 0, runtime * 1000, TaskID.forName("task_201402260232_111112_r_000000"));
        mappers[2] = new HadoopTaskData(counter_c, 0, runtime * 1000, TaskID.forName("task_201402260232_111112_r_000000"));

        reducers[0] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111113_r_000000"));
        reducers[1] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111114_r_000000"));
        reducers[2] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111114_r_000000"));

        HadoopJobData data = new HadoopJobData(counters, mappers, reducers);

        HeuristicResult result = heuristic.apply(data);

        assertFalse(result.succeeded());
    }

    public void testApplySuccess() throws Exception {
        HadoopCounterHolder counters = new HadoopCounterHolder();
        HadoopTaskData[] mappers = new HadoopTaskData[3];
        HadoopTaskData[] reducers = new HadoopTaskData[3];

        HadoopCounterHolder counter_a = new HadoopCounterHolder();
        HadoopCounterHolder counter_b = new HadoopCounterHolder();
        HadoopCounterHolder counter_c = new HadoopCounterHolder();

        //100 MB/s

        long runtime = 20 * 60; // 20 minutes

        counter_a.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 100 * 1024 * 1024); //100MB
        counter_b.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 100 * 1024 * 1024); //100MB
        counter_c.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 100 * 1024 * 1024); //100MB

        mappers[0] = new HadoopTaskData(counter_a, 0, runtime * 1000, TaskID.forName("task_201402260232_111111_r_000000"));
        mappers[1] = new HadoopTaskData(counter_b, 0, runtime * 1000, TaskID.forName("task_201402260232_111112_r_000000"));
        mappers[2] = new HadoopTaskData(counter_c, 0, runtime * 1000, TaskID.forName("task_201402260232_111112_r_000000"));

        reducers[0] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111113_r_000000"));
        reducers[1] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111114_r_000000"));
        reducers[2] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111114_r_000000"));

        HadoopJobData data = new HadoopJobData(counters, mappers, reducers);

        HeuristicResult result = heuristic.apply(data);

        assertTrue(result.succeeded());
    }

    public void testApplyIgnoredSuccess() throws Exception {
        HadoopCounterHolder counters = new HadoopCounterHolder();
        HadoopTaskData[] mappers = new HadoopTaskData[3];
        HadoopTaskData[] reducers = new HadoopTaskData[3];

        HadoopCounterHolder counter_a = new HadoopCounterHolder();
        HadoopCounterHolder counter_b = new HadoopCounterHolder();
        HadoopCounterHolder counter_c = new HadoopCounterHolder();

        //2MB/s
        long runtime = 60; // 1 minute

        counter_a.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 2 * 1024 * 1024);
        counter_b.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 2 * 1024 * 1024);
        counter_c.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, runtime * 2 * 1024 * 1024);

        mappers[0] = new HadoopTaskData(counter_a, 0, runtime * 1000, TaskID.forName("task_201402260232_111111_r_000000"));
        mappers[1] = new HadoopTaskData(counter_b, 0, runtime * 1000, TaskID.forName("task_201402260232_111112_r_000000"));
        mappers[2] = new HadoopTaskData(counter_c, 0, runtime * 1000, TaskID.forName("task_201402260232_111112_r_000000"));

        reducers[0] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111113_r_000000"));
        reducers[1] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111114_r_000000"));
        reducers[2] = new HadoopTaskData(counters, 0, 1000, TaskID.forName("task_201402260232_111114_r_000000"));

        HadoopJobData data = new HadoopJobData(counters, mappers, reducers);

        HeuristicResult result = heuristic.apply(data);

        assertTrue(result.succeeded());
    }
}
