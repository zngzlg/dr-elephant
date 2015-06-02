package org.apache.spark.deploy.history


import java.net.URI
import java.security.PrivilegedAction

import com.linkedin.drelephant.HadoopSecurity
import com.linkedin.drelephant.analysis.ElephantFetcher
import com.linkedin.drelephant.spark.SparkApplicationData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, FileStatus}
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus, ApplicationEventListener}
import org.apache.spark.storage.{StorageStatusTrackingListener, StorageStatusListener}
import org.apache.spark.ui.env.EnvironmentListener
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.storage.StorageListener


/**
 * A wrapper that replays Spark event history from files and then fill proper data objects.
 *
 * @author yizhou
 */
class SparkFSFetcher extends ElephantFetcher[SparkApplicationData] {


  import SparkFSFetcher._


  /* Lazy loading for the log directory is very important. Hadoop Configuration() takes time to load itself to reflect
   * properties in the configuration files. Triggering it too early will sometimes make the configuration object empty.
   */
  private lazy val _logDir: String = {
    val conf = new Configuration()
    val nodeAddress = conf.get("dfs.namenode.http-address", null)
    val hdfsAddress = if (nodeAddress == null) "" else "webhdfs://" + nodeAddress

    val uri = new URI(_sparkConf.get("spark.eventLog.dir", DEFAULT_LOG_DIR))
    System.out.println("logDIR:    " + hdfsAddress + uri.getPath)
    hdfsAddress + uri.getPath
  }

  private val _sparkConf = new SparkConf()
  private val _security = new HadoopSecurity()

  private lazy val fs: FileSystem = {
    _security.checkLogin()
    logger.info("Looking for spark logs at " + _logDir)

    if (new URI(_logDir).getHost == null) {
      FileSystem.getLocal(new Configuration())
    } else {
      val filesystem = new WebHdfsFileSystem()
      filesystem.initialize(new URI(_logDir), new Configuration())
      filesystem
    }
  }

  def fetchData(appId: String): SparkApplicationData = {
    _security.doAs[SparkDataCollection](new PrivilegedAction[SparkDataCollection] {
      override def run(): SparkDataCollection = {
        /* Most of Spark logs will be in directory structure: /LOG_DIR/[application_id].
         *
         * Some logs (Spark 1.3+) are in /LOG_DIR/[application_id].snappy
         *
         * Currently we won't be able to parse them even we manually set up the codec. There is problem
         * in JsonProtocol#sparkEventFromJson that it does not handle unmatched SparkListenerEvent, which means
         * it is only backward compatible but not forward. And switching the dependency to Spark 1.3 will raise more
         * problems due to the fact that we are touching the internal codes.
         *
         * In short, this fetcher only works with Spark <=1.2, and we should switch to JSON endpoints with Spark's
         * future release.
         */
        val replayBus = createReplayBus(fs.getFileStatus(new Path(_logDir, appId)))
        val applicationEventListener = new ApplicationEventListener
        val jobProgressListener = new JobProgressListener(new SparkConf())
        val environmentListener = new EnvironmentListener
        val storageStatusListener = new StorageStatusListener
        val executorsListener = new ExecutorsListener(storageStatusListener)
        val storageListener = new StorageListener(storageStatusListener)

        // This is a customized listener that tracks peak used memory
        // The original listener only tracks the current in use memory which is useless in offline scenario.
        val storageStatusTrackingListener = new StorageStatusTrackingListener()
        replayBus.addListener(storageStatusTrackingListener)

        val dataCollection = new SparkDataCollection(applicationEventListener = applicationEventListener,
          jobProgressListener = jobProgressListener,
          environmentListener = environmentListener,
          storageStatusListener = storageStatusListener,
          executorsListener = executorsListener,
          storageListener = storageListener,
          storageStatusTrackingListener = storageStatusTrackingListener)

        replayBus.addListener(applicationEventListener)
        replayBus.addListener(jobProgressListener)
        replayBus.addListener(environmentListener)
        replayBus.addListener(storageStatusListener)
        replayBus.addListener(executorsListener)
        replayBus.addListener(storageListener)

        replayBus.replay()

        dataCollection
      }
    })
  }

  private def createReplayBus(logDir: FileStatus): ReplayListenerBus = {
    val path = logDir.getPath()
    val elogInfo = EventLoggingListener.parseLoggingInfo(path, fs)
    new ReplayListenerBus(elogInfo.logPaths, fs, elogInfo.compressionCodec)
  }
}

private object SparkFSFetcher {
  private val logger = Logger.getLogger(SparkFSFetcher.getClass)
  val DEFAULT_LOG_DIR = "/system/spark-history"
}
