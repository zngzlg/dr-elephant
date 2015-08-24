/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.drelephant;

import com.linkedin.events.KafkaAuditHeader;
import com.linkedin.events.hadoop.AggregationScope;
import com.linkedin.events.hadoop.AzkabanContext;
import com.linkedin.events.hadoop.Counter;
import com.linkedin.events.hadoop.ExecutionContext;
import com.linkedin.events.hadoop.HadoopHeader;
import com.linkedin.events.hadoop.HadoopMetricsEvent;
import com.linkedin.events.hadoop.State;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.UUID;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import play.Play;


/**
 * TODO Decide if the contained classes should be split out.
 *
 * This class provides a set of classes that can be used to construct and publish hadoop
 * MetricsEvent. The inner classes are:
 *
 * CountersToPublish: This class takes in a hadoop configuration value (string) that indicates which counters are to be published
 *    when the job finishes. The key for this configuration is defined in the JobProperties class.
 * EventContext: This is one of the two objects that an application constructs if a metrics event is to be published. The event context
 *    has the start and end time window in which the metrics were gathered,and also the job name and container ID.
 * JobProperties: THis is the other object that the application needs to construct if a metrics event is to be published.
 *    It is filled in from the hadoop job configuration. It has the azkaban related fields, and a couple of other config
 *    properties. It also has the config property of the counters to publish. JobProperties uses that to construct
 *    CountersToPublish
 * HadoopCounters: Represents the metrics event that we need to publish. In the final API exposed to (map-reduce) applications,
 *    I expect another class called HadoopMetrics (or some such) that allows applications to add counters, gauges, etc.
 *    Application must construct this class (with JobProperties and EventContext) in order to publish an event.
 *    The build() method in this class returns an avro IndexedRecord that can then be handed to the publisher to publish.
 * CommonEventsHeaderBuilder: A private class used by the metrics library to construct the kafka audit header.
 *
 * MetricsPublisher: An interface that every event publisher must implement. Has a publish() method to publish an
 *    IndexedRecord.
 * HDFSMetricsPublisher: Implements a class that publishes metrics to HDFS. It is constructed via a static method
 *    (should really be in a factory) that takes an XML file as input. The XML file has configuration parameters for
 *    the HDFS publisher (e.g. the root directory under which events should be published).
 */
public class DaliMetricsAPI {
  private static final Logger LOG = Logger.getLogger(DaliMetricsAPI.class);

  /**
   * A helper class to keep track of which metrics to publish.
   */
  public static class CountersToPublish {
    private static final String ALL_COUNTERS = "*";
    private final Map<String, Set<String>> _countersToPublish;
    /**
     * @param counterNames A string of the format "groupName:counterName,groupName:counterName,..."
     *                     counterName can be '*' meaning all counters in the group.
     * @return A map with one key per counter group -- the value being a set
     */
    public CountersToPublish(String counterNames) {
      Map<String, Set<String>> countersToPublish = null;

      if (counterNames != null && counterNames.length() != 0) {
        countersToPublish = new HashMap<String, Set<String>>(4);
        StringTokenizer st = new StringTokenizer(counterNames, ",");

        while (st.hasMoreTokens()) {
          String val = st.nextToken();
          String[] groupCounter = val.split(":");
          if (groupCounter.length != 2) {
            // Malformed group+counter
            LOG.error("Ignoring malformed group/counter pair:'" + val + "'");
            continue;
          }
          if (groupCounter[1] == null) {
            // TODO
            LOG.error("Ignoring malformed counter name:'" + val + "'");
            continue;
          }
          String groupName = groupCounter[0];
          String counterName = groupCounter[1];
          if (!countersToPublish.containsKey(groupName)) {
            countersToPublish.put(groupName, new HashSet<String>(4));
          }
          countersToPublish.get(groupName).add(counterName);
        }
      }
      _countersToPublish = countersToPublish;
    }

    /*
     * Returns true if no counters need to be published.
     */
    public boolean isEmpty() {
      return _countersToPublish == null;
    }

    /*
     * Returns true if the specified counter is to be published.
     */
    public boolean hasCounter(String groupName, String counterName) {
      if (_countersToPublish == null) {
        return false;
      }
      Set<String> countersInGroup = _countersToPublish.get(groupName);
      if (countersInGroup == null) {
        return false;
      }
      if (countersInGroup.contains(ALL_COUNTERS) || countersInGroup.contains(counterName)) {
        return true;
      }
      return false;
    }
  }

  /**
   * EventContext is a helper class that the application needs to fill in before
   * requesting to publish counters for a job.
   *
   * In the more general case of a task publishing metrics, we need to expand this
   * to contain a builder that can populate the members.
   *
   * Keeping code to a minimum for now.
   */
  public static class EventContext {
    private final String _containerId;
    private final AggregationScope _scope;
    private final State _state;
    private final String _jobName;
    private final String _jobId;
    private final long _startTime;
    private final long _endTime;

    /**
     * For now, we only publish for successful jobs, so this constructor works.
     * @param jobName hadoop job name from JobReport
     * @param jobId hadoop job Id from JobReport
     */
    public EventContext(String jobName, String jobId, long startTime, long endTime) {
      _containerId = null;
      _scope = AggregationScope.JOB;
      _state = State.SUCCEEDED;
      _jobName = jobName;
      _jobId = jobId;
      _startTime = startTime;
      _endTime = endTime;
    }

    public String getContainerId() {
      return _containerId;
    }

    public String getJobName() {
      return _jobName;
    }

    public String getJobId() {
      return _jobId;
    }

    public State getState() {
      return _state;
    }

    public AggregationScope getScope() {
      return _scope;
    }

    public long getStartTime() {
      return _startTime;
    }

    public long getEndTime()  {
      return _endTime;
    }

    /*
     * TODO
     * - Add a builder (setSuccess(), setJobScope() etc. may make more sense than exposing 'State', for example)
     * - May be sub-class with a container Id for task-level API?
     */
  }

  /**
   * JobProperties is a helper class that the application needs to fill in before
   * requesting to publish counters for a job.
   *
   * Inside the cluster, Configuration can be used to populate this.
   */
  public static class JobProperties {
    private static final String UNKNOWN_CLUSTERNAME = "Unknown";
    private static final String UNKNOWN_USERNAME = "Unknown";

    private static final String CONFIG_KEY_PUB_COUNTERS = "mapreduce.job.publish-counters";
    private static final String CONFIG_KEY_AZ_JOBID = "azkaban.job.id";
    private static final String CONFIG_KEY_AZ_FLOWID = "azkaban.flow.flowid";
    private static final String CONFIG_KEY_AZ_PROJNAME = "azkaban.flow.projectname";
    private static final String CONFIG_KEY_AZ_PROJVER = "azkaban.flow.projectversion";
    private static final String CONFIG_KEY_AZ_PROJ_LAST_CHANGED_DATE = "azkaban.flow.projectlastchangeddate";
    private static final String CONFIG_KEY_AZ_EXECURL = "azkaban.link.jobexec.url";
    private static final String CONFIG_KEY_AZ_EXECID = "azkaban.flow.execid";
    private static final String CONFIG_KEY_NN_HOSTPORT = "dfs.namenode.http-address";
    private static final String CONFIG_KEY_USER_NAME = "mapreduce.job.user.name";

    private final CountersToPublish _countersToPublish;
    private final String _azkabanJobId;
    private final String _azkabanFlowId;
    private final String _azkabanProjectName;
    private final String _azkabanProjectVersion;
    private final long _azkabanProjectLastChangedDate;
    private final String _azkabanHostName;
    private final int _execId;
    private final String _clusterName;
    private final String _userName;

    public JobProperties(Properties properties) {
      _countersToPublish = new CountersToPublish(properties.getProperty(CONFIG_KEY_PUB_COUNTERS));
      _azkabanJobId = properties.getProperty(CONFIG_KEY_AZ_JOBID);
      _azkabanFlowId = properties.getProperty(CONFIG_KEY_AZ_FLOWID);
      _execId = decodeExecId(properties.getProperty(CONFIG_KEY_AZ_EXECID));
      _azkabanProjectName = properties.getProperty(CONFIG_KEY_AZ_PROJNAME);
      _azkabanProjectVersion = properties.getProperty(CONFIG_KEY_AZ_PROJVER);
      _azkabanProjectLastChangedDate = parseDate(properties.getProperty(CONFIG_KEY_AZ_PROJ_LAST_CHANGED_DATE));
      _azkabanHostName = url2HostName(properties.getProperty(CONFIG_KEY_AZ_EXECURL));
      _clusterName = nnHostPort2ClusterName(properties.getProperty(CONFIG_KEY_NN_HOSTPORT));
      _userName = properties.getProperty(CONFIG_KEY_USER_NAME);
    }

    // TODO Add a constructor with org.apache.hadoop.mapreduce.v2.api.records.JobReport for use inside cluster?

    /*
     * We get a string that should parse into a long value of msecs since epoch.
     */
    private long parseDate(final String msecsSinceEpoch) {
      long secsSinceEpoch = -1;
      if (msecsSinceEpoch == null) {
        return secsSinceEpoch;
      }
      try {
        secsSinceEpoch = Long.parseLong(msecsSinceEpoch)/1000L;
      } catch (NumberFormatException e) {
        LOG.warn("Could not parse msecs since epoch " + msecsSinceEpoch);
      }
      return secsSinceEpoch;
    }

    private int decodeExecId(String execIdStr) {
      int execId = -1;
      if (execIdStr != null) {
        try {
          execId = Integer.parseInt(execIdStr);
        } catch (NumberFormatException e) {
          // TODO Log the exception?
          LOG.warn("Illegal value for execId:'" + execIdStr + "'");
        }
      }
      return execId;
    }

    private String url2HostName(String urlStr) {
      String hostName = null;
      if (urlStr != null && urlStr.length() > 0) {
        try {
          URL url = new URL(urlStr);
          hostName = url.getHost();
        } catch (MalformedURLException e) {
          // TODO TBD log the exception?
          LOG.warn("Could not determine hostname from URL:'" + urlStr + "'");
        }
      }
      return hostName;
    }

    /**
     * Helper method to map a name node URL to a cluster name. It is assumed that the URL is of the form
     *    datacenter-clusternamennNN.grid.linkedin.com (where N is a digit, 'clustername' is the name of the cluster)
     * @param hostPort
     * @return name of the cluster or "Unknown" if cluster name cannot be determined.
     */
    private String nnHostPort2ClusterName(String hostPort) {
      if (hostPort == null || hostPort.length() == 0) {
        return UNKNOWN_CLUSTERNAME;
      }
      String hostName = url2HostName("http://" + hostPort);
      if (hostName == null) {
        return UNKNOWN_CLUSTERNAME;
      }
      // Assume the hostname is of the type <datacenter>-<clustername>nn<digit><digit>.grid.linkedin.com
      // May not work all the time!
      String hostNameSansDomain = hostName.replace(".grid.linkedin.com", "");
      if (hostNameSansDomain.length() < 7) {
        // need 4 chars for nn<d><d> and one char for '-' and at least one char for datacenter, and one char for clusterName
        return UNKNOWN_CLUSTERNAME;
      }
      int clusterNameStart = hostNameSansDomain.indexOf('-');
      if (clusterNameStart == -1) { // There is no dash here, just return unknown clustername
        return UNKNOWN_CLUSTERNAME;
      }
      clusterNameStart++;
      int clusterNameEnd = hostNameSansDomain.length()-4;
      return hostNameSansDomain.substring(clusterNameStart, clusterNameEnd);
    }

    public CountersToPublish getCountersToPublish() {
      return _countersToPublish;
    }

    public long getAzkabanProjectLastChangedDate() {
      return _azkabanProjectLastChangedDate;
    }

    public int getExecId() {
      return _execId;
    }

    public String getAzkabanHostName(final String defaultValue) {
      return _azkabanHostName == null ? defaultValue : _azkabanHostName;
    }

    public String getAzkabanProjectVersion(final String defaultValue) {
      return _azkabanProjectVersion == null ? defaultValue : _azkabanProjectVersion;
    }

    public String getAzkabanFlowId(final String defaultValue) {
      return _azkabanFlowId == null ? defaultValue : _azkabanFlowId;
    }

    public String getAzkabanJobId(final String defaultValue) {
      return _azkabanJobId == null ? defaultValue : _azkabanJobId;
    }

    public String getAzkabanProjectName(final String defaultValue) {
      return _azkabanProjectName == null ? defaultValue : _azkabanProjectName;
    }

    public String getClusterName() {
      return _clusterName;
    }

    public String getUserName() {
      return _userName;
    }

    // If any of the properties are set, then the job is coming through azkaban.
    public boolean hasAnyAzkabanProperties() {
      return _execId != -1 ||
          _azkabanFlowId != null ||
          _azkabanHostName != null ||
          _azkabanJobId != null ||
          _azkabanProjectLastChangedDate != -1L ||
          _azkabanProjectName != null ||
          _azkabanProjectVersion != null;
    }

    // TODO Add a constructor from org.apache.hadoop.conf.Configuration
  }

  /**
   * A class that the applications can use to construct the HadoopMetricsEvent avro
   * event and hand it to the publisher to publish.
   *
   * This class takes care of populating the headers from the data given, and provides methods
   * to add counters to the event.
   */
  public static class HadoopCounters {
    private final CountersToPublish _countersToPublish;
    private final HadoopMetricsEvent _hadoopMetricsEvent;
    private final List<Counter> _counters;
    private int _numCounters = 0;

    public HadoopCounters(EventContext eventContext, JobProperties jobProperties) {
      _countersToPublish = jobProperties.getCountersToPublish();

      _counters = new LinkedList<Counter>();

      AzkabanContext azkabanContext = null;
      // Fill in Azkaban Context
      if (jobProperties.hasAnyAzkabanProperties()) {
        azkabanContext = new AzkabanContext();
        azkabanContext.executionId = jobProperties.getExecId();
        azkabanContext.flowName = jobProperties.getAzkabanFlowId("Unknown");
        azkabanContext.hostName = jobProperties.getAzkabanHostName("Unknown");
        azkabanContext.lastChangedDate = jobProperties.getAzkabanProjectLastChangedDate();
        azkabanContext.projectName = jobProperties.getAzkabanProjectName("Unknown");
        azkabanContext.projectVersion = jobProperties.getAzkabanProjectVersion("Unknown");
        azkabanContext.jobName = jobProperties.getAzkabanJobId("Unknown");
      }


      ExecutionContext executionContext = new ExecutionContext();
      executionContext = new ExecutionContext();
      executionContext.applicationId = eventContext.getJobId();
      executionContext.applicationName = eventContext.getJobName();
      executionContext.state = eventContext.getState();
      executionContext.containerId = eventContext.getContainerId();
      executionContext.userName = jobProperties.getUserName();
      executionContext.clusterName = jobProperties.getClusterName();

      HadoopHeader hadoopHeader = new HadoopHeader();
      hadoopHeader = new HadoopHeader();
      hadoopHeader.aggregationScope = eventContext.getScope();
      hadoopHeader.startTime = eventContext.getStartTime();
      hadoopHeader.endTime = eventContext.getEndTime();
      hadoopHeader.azkabanContext = azkabanContext;
      hadoopHeader.executionContext = executionContext;

      _hadoopMetricsEvent = new HadoopMetricsEvent();
      _hadoopMetricsEvent.hadoopHeader = hadoopHeader;
      _hadoopMetricsEvent.auditHeader = new CommonEventHeaderBuilder().build();
   }

    /**
     * Add a counter to the list of counters to be published, if the counter is configured to be
     * published. If not, then do nothing.
     *
     * TODO Replace a counter that is already added? Or throw error?
     *
     * @param groupName
     * @param counterName
     * @param value
     */
    public void addCounter(String groupName, String counterName, long value) {
      if (_countersToPublish.hasCounter(groupName, counterName)) {
        Counter counter = new Counter();
        counter.counterGroup = groupName;
        counter.counterName = counterName;
        counter.value = value;
        _counters.add(counter);
        _numCounters++;
      }
    }

    /**
     * @return the total number of counters added to the publish list.
     */
    public int getNumCounters() {
      return _numCounters;
    }

    /**
     * Build the avro event and return an IndexedRecord.
     * @note It is not guaranteed that this record will be serialized correctly, since no verification is done here
     * TODO Add verification so that avro publish will not fail. Or, return a serialized bytebuffer?
     * @return
     */
    public IndexedRecord build() {
      _hadoopMetricsEvent.counters = _counters;
      return _hadoopMetricsEvent;
    }
  }

  /**
   * A helper class to fill in the kafka audit header.
   * This class should not be exposed outside the DaliMetrics API.
   */
  static class CommonEventHeaderBuilder {
    private static final String HADOOP_SERVICE = "hadoop";
    private static final String UNKNOWN_HOSTNAME = "Unknown";

    private static String hostName = null;

    private final KafkaAuditHeader _auditHeader;
    public CommonEventHeaderBuilder() {
      _auditHeader = new KafkaAuditHeader();
    }
    public KafkaAuditHeader build() {
      _auditHeader.time = System.currentTimeMillis();
      _auditHeader.server = getHostName();
      _auditHeader.appName = HADOOP_SERVICE;
      byte[] bytes = getUUID();
      com.linkedin.events.UUID liUUID = new com.linkedin.events.UUID();
      System.arraycopy(bytes, 0, liUUID.bytes(), 0, bytes.length);
      _auditHeader.messageId = liUUID;

      return _auditHeader;
    }

    /**
     * A helper method to get (and cache) the hostname
     * @return name of this host or "Unknown" if there are errors
     */
    private synchronized static String getHostName() {

      if (hostName != null) {
        return hostName;
      }
      try {
        hostName = InetAddress.getLocalHost().getHostName();
        return hostName;
      } catch (UnknownHostException e) {
        LOG.warn("Could not determine hostname", e);
        return UNKNOWN_HOSTNAME;
      }
    }

    /**
     * A helper method to return a 16-byte UUID
     * @return UUID of 16 bytes
     */
    private byte[] getUUID() {
      final UUID uuid = UUID.randomUUID();
      final ByteBuffer bb = ByteBuffer.allocate(16);  // Default is big-endian
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    }

  }

  /**
   * This interface us to be implemented by all the publishers.
   */
  public interface MetricsPublisher {
    /**
     * A thread-safe publish method that publishes an event
     * @param event
     * @throws IOException
     */
    public void publish(IndexedRecord event) throws IOException;
  }

  /**
   *  An implementation of a publisher to publish to HDFS file system.
   */
  public static class HDFSMetricsPublisher implements MetricsPublisher {
    private static String DEFAULT_BASE_PATH = "/data/HadoopMetrics/jobs";

    private final String _basePath;

    private HDFSMetricsPublisher(String basePath) {
      if (basePath.endsWith("/")) {
        _basePath = basePath;
      } else {
        _basePath = basePath + "/";
      }
    }

    /**
     * @param configFileName
     * @return null if the config file does not exist or hdfs publishers not configured, or any misconfiguration is detected.
     */
    public static MetricsPublisher createFromXml(String configFileName) {
      MetricsPublisher metricsPublisher = null;
      if (configFileName != null) {
        InputStream instream = null;
        LOG.info("Attempting to read metrics publish configuration from " + configFileName);
        instream = Play.application().resourceAsStream(configFileName);
        if (instream == null) {
          LOG.info("Configuation file not present in classpath. File: " + configFileName);
          return null;
        }
        LOG.info("Configuation file loaded. File: " + configFileName);
        metricsPublisher = getHdfsMetricsPublisher(configFileName, instream);
      }
      if (metricsPublisher == null) {
        LOG.info("Not valid config so metrics will not be published.");
      }
      return metricsPublisher;
    }

    private static HDFSMetricsPublisher getHdfsMetricsPublisher(String logmsgPrefix, InputStream instream) {
      try {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(instream);
        NodeList publishers = document.getElementsByTagName("publisher");
        if (publishers.getLength() == 0) {
          LOG.info("No publishers configured");
          return null;
        }
        Element publisherNode = (Element)publishers.item(0);
        NodeList schemes = publisherNode.getElementsByTagName("scheme");
        if (schemes.getLength() == 0) {
          LOG.info("No publish scheme configured");
          return null;
        }
        Element schemeElement = (Element)schemes.item(0);
        if (schemeElement.getTextContent() == null ||
            !schemeElement.getTextContent().equals("hdfs")) {
          LOG.info("Publish scheme must be \"hdfs\"");
          return null;
        }
        NodeList basePathNodes = publisherNode.getElementsByTagName("basepath");
        if (basePathNodes == null || basePathNodes.getLength() == 0) {
          LOG.info("HDFS Base path not configured, assumed " + DEFAULT_BASE_PATH);
          return new HDFSMetricsPublisher(DEFAULT_BASE_PATH);
        }
        Element basePathElement = (Element)basePathNodes.item(0);
        if (basePathElement == null || basePathElement.getTextContent()== null) {
          LOG.info("HDFS Base path not configured, assumed " + DEFAULT_BASE_PATH);
          return new HDFSMetricsPublisher(DEFAULT_BASE_PATH);
        }
        String basePath = basePathElement.getTextContent();
        if (!basePath.startsWith("/")) {
          LOG.error("Ignoring illegal value for basepath:'" + basePath + "'(must start with '/'). Using "
              + DEFAULT_BASE_PATH + " instead");
          return new HDFSMetricsPublisher(DEFAULT_BASE_PATH);
        }
        return new HDFSMetricsPublisher(basePath);
      } catch (ParserConfigurationException e) {
        throw new RuntimeException(logmsgPrefix + ":Exception during parse", e);
      } catch (SAXException e) {
        throw new RuntimeException(logmsgPrefix + ":Exception during parse", e);
      } catch (IOException e) {
        throw new RuntimeException(logmsgPrefix + ":Exception during parse", e);
      }
    }

    private Object extractField(final IndexedRecord indexedRecord,
                                final String fieldName,
                                final boolean mustBeRecord)
      throws IOException {

      final Schema schema = indexedRecord.getSchema();
      final Schema.Field  field = schema.getField(fieldName);
      if (field == null) {
        throw new IOException(fieldName + ":No such field in " + schema.getName());
      }
      final int fieldPos = field.pos();
      final Object fieldValue = indexedRecord.get(fieldPos);
      if (fieldValue == null) {
        throw new IOException(fieldName + ":Null value in " + schema.getName());
      }
      if (mustBeRecord) {
        final Schema fieldSchema = field.schema();
        if (fieldSchema == null) {
          throw new IOException(fieldName + ":No such record in " + schema.getName());
        }
      }

      return fieldValue;
    }

    private long extractPublishTime(IndexedRecord event)
      throws IOException {

      IndexedRecord auditHeader = (IndexedRecord)extractField(event, "auditHeader", true);
      Long time = (Long)extractField(auditHeader, "time", false);
      return time;
    }

    private String extractApplicationId(IndexedRecord event)
      throws IOException {

      IndexedRecord hadoopHeader = (IndexedRecord)extractField(event, "hadoopHeader", true);
      IndexedRecord executionContext = (IndexedRecord)extractField(hadoopHeader, "executionContext", true);
      String applicationId = (String)extractField(executionContext, "applicationId", false);
      return applicationId;
    }

    @Override
    public void publish(IndexedRecord event)
        throws IOException {

      long pubTime = extractPublishTime(event);
      String applicationId = extractApplicationId(event);
      String path = getPathName(pubTime, applicationId);

      OutputStream outStream = getOutputStream(path);

      writeToStream(outStream, event);
    }

    private OutputStream getOutputStream(String pathName)
        throws IOException {

      Configuration config = new Configuration();
      // TODO Decide permissions correctly. Right now, this is world-reasable, but perhaps it should be group-readable?
      FsPermission perms = new FsPermission((short)0644);
      Path path = new Path(pathName);
      FileSystem fs = null;
      try {
        fs = path.getFileSystem(config);
      } catch (IOException e) {
        LOG.error("Could not get file system", e);
        throw e;
      }
      LOG.debug("Publishing metrics to " + fs + ", path=" + path);

      try {
        FSDataOutputStream fileOutputStream = FileSystem.create(fs, path, perms);
        return fileOutputStream;
      } catch (IOException e) {
        LOG.error("Exception writing to avro file", e);
        throw e;
      }
    }

    private String getPathName(long pubTime, String fileName) {
      GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      cal.setTime(new Date(pubTime));
      String year = Integer.toString(cal.get(Calendar.YEAR));
      String month = String.format("%02d", cal.get(Calendar.MONTH) + 1);  // Month seems to start from 0!
      String day = String.format("%02d", cal.get(Calendar.DATE));
      String hour = String.format("%02d", cal.get(Calendar.HOUR_OF_DAY));
      String path =  _basePath + "/hourly/" + year + "/" + month + "/" + day +  "/" + hour + "/" + fileName + ".avro";
      return path;
    }

    private void writeToStream(OutputStream outStream, IndexedRecord event)
        throws IOException {
      DatumWriter<IndexedRecord> dw = new GenericDatumWriter<IndexedRecord>(event.getSchema());
      DataFileWriter<IndexedRecord> dfw = new DataFileWriter<IndexedRecord>(dw);
      dfw.create(event.getSchema(), outStream);
      dfw.append(event);
      dfw.close();
    }
  }

  // For testing purposes
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.setProperty(JobProperties.CONFIG_KEY_PUB_COUNTERS, "fruits:*");
    props.setProperty(JobProperties.CONFIG_KEY_AZ_EXECID, "21345");
    props.setProperty(JobProperties.CONFIG_KEY_AZ_EXECURL, "http://host:6556/exec/url");
    props.setProperty(JobProperties.CONFIG_KEY_AZ_FLOWID, "flowId");
    props.setProperty(JobProperties.CONFIG_KEY_AZ_JOBID, "jobId");
    props.setProperty(JobProperties.CONFIG_KEY_AZ_PROJ_LAST_CHANGED_DATE, "2014-12-10");
    props.setProperty(JobProperties.CONFIG_KEY_AZ_PROJNAME, "projName");
    props.setProperty(JobProperties.CONFIG_KEY_AZ_PROJVER, "v1.3");
    props.setProperty(JobProperties.CONFIG_KEY_NN_HOSTPORT, "http://eat1-magicnn01:8723/name/node");
    props.setProperty(JobProperties.CONFIG_KEY_USER_NAME, "user");
    JobProperties jp = new JobProperties(props);

    EventContext ec = new EventContext("jobName", "jobId", 123456L, 234567L);

    HadoopCounters hm = new HadoopCounters(ec, jp);
    hm.addCounter("fruits", "apple", 45);
    IndexedRecord hme = hm.build();
    publish(hme);
  }

  public static void publish(IndexedRecord event)
      throws IOException {
    String path = "/tmp/xx.avro";
    OutputStream outStream = new FileOutputStream(new File(path));

    DatumWriter<IndexedRecord> dw = new GenericDatumWriter<IndexedRecord>(event.getSchema());
    DataFileWriter<IndexedRecord> dfw = new DataFileWriter<IndexedRecord>(dw);
    dfw.create(event.getSchema(), outStream);
    dfw.append(event);
    dfw.close();
  }

}
