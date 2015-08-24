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
package com.linkedin.drelephant.util;

import com.linkedin.drelephant.DaliMetricsAPI;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import play.Play;


public final class Utils {
  private static final Logger logger = Logger.getLogger(Utils.class);
  // Matching x.x.x or x.x.x-li1 (x are numbers)
  public static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)(?:\\.\\d+)*(?:\\-[\\dA-Za-z]+)?");

  private Utils() {
    // do nothing
  }

  /**
   * Given a mapreduce job's application id, get its corresponding job id.
   *
   * @param appId
   * @return the corresponding job id
   */
  public static String getJobIdFromApplicationId(String appId) {
    return appId.replaceAll("application", "job");
  }

  /**
   * Given a MapreduceApplicationData instance, publish the corresponding Dali Metrics if needed.
   *
   * @param jobData the data to check for publishing
   */
  public static void publishMetrics(MapReduceApplicationData jobData) {
    DaliMetricsAPI.MetricsPublisher metricsPublisher = ElephantContext.instance().getMetricsPublisher();
    if (metricsPublisher == null) {
      return;
    }

    Properties jobConf = jobData.getConf();
    // We may have something to publish, but we don't know until we have iterated through the counters that we have.
    // We assume that we need to publish something until we find out we don't.
    DaliMetricsAPI.JobProperties jobProperties = new DaliMetricsAPI.JobProperties(jobConf);
    if (jobProperties.getCountersToPublish().isEmpty()) {
      // Nothing to do
      return;
    }
    logger.info("Publishing counters for job[" + jobData.getJobId() + "]");
    DaliMetricsAPI.EventContext eventContext =
        new DaliMetricsAPI.EventContext(jobData.getJobName(), jobData.getJobId(), jobData.getStartTime(),
            jobData.getFinishTime());
    DaliMetricsAPI.HadoopCounters metricsEvent = new DaliMetricsAPI.HadoopCounters(eventContext, jobProperties);

    MapReduceCounterHolder counterHolder = jobData.getCounters();
    logger.info("HadoopCounterHolder: {" + counterHolder + "}");
    Set<String> groupNames = counterHolder.getGroupNames();
    logger.info("group names: [" + StringUtils.join(groupNames, ",") + "]");
    for (String group : groupNames) {
      Map<String, Long> counters = counterHolder.getAllCountersInGroup(group);
      for (Map.Entry<String, Long> entry : counters.entrySet()) {
        String counterName = entry.getKey();
        Long value = entry.getValue();
        logger.info(String.format("%s,,,,,,,%s", counterName, String.valueOf(value)));
        metricsEvent.addCounter(group, counterName, value);
      }
    }

    if (metricsEvent.getNumCounters() == 0) {
      logger.info("No counters need to be published for job [" + jobData.getJobId() + "]");
      // The counters that were configured were not collected in HadoopCounterHolder.
      return;
    }
    IndexedRecord event = metricsEvent.build();
    try {
      metricsPublisher.publish(event);
    } catch (IOException e) {
      // The lower level should have logged a message.
      // A checked exception from the publish() call should mean that the event was not formed correctly for some reason.
      // Aside from a code bug, the most common reason for this will probably be that some mandatory fields in the
      // event were missing.
      // Could also mean that some derived values (e.g. hostname to URL, execId translation to an integer, etc.)
      // may have failed. There is little we can do at this point to fix those, so ignore the exception.
      logger.log(Level.WARN, "Publish failed:", e);
    }
  }

  /**
   * Load an XML document from a file path
   *
   * @param filePath The file path to load
   * @return The loaded Document object
   */
  public static Document loadXMLDoc(String filePath) {
    InputStream instream = null;
    logger.info("Loading configuration file " + filePath);
    instream = Play.application().resourceAsStream(filePath);

    if (instream == null) {
      logger.info("Configuation file not present in classpath. File:  " + filePath);
      throw new RuntimeException("Unable to read " + filePath);
    }
    logger.info("Configuation file loaded. File: " + filePath);

    Document document = null;
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      document = builder.parse(instream);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("XML Parser could not be created.", e);
    } catch (SAXException e) {
      throw new RuntimeException(filePath + " is not properly formed", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read " + filePath, e);
    }

    return document;
  }

  /**
   * Given a version string e.g.: 1.2.3, return the corresponding major version number
   *
   * @param versionString the version string
   * @return the major version number
   */
  public static int getMajorVersionFromString(String versionString) {
    if (versionString == null) {
      throw new IllegalArgumentException("Cannot get major version from null.");
    }

    Matcher matcher = VERSION_PATTERN.matcher(versionString);
    if (!matcher.matches() || matcher.groupCount() < 1) {
      throw new IllegalArgumentException(
          "Version string [" + versionString + "] does not match pattern + [" + VERSION_PATTERN + "].");
    }

    return Integer.parseInt(matcher.group(1));
  }

  /**
   * Parse a java option string in the format of "-Dfoo=bar -Dfoo2=bar ..." into a {optionName -> optionValue} map.
   *
   * @param str The option string to parse
   * @return A map of options
   */
  public static Map<String, String> parseJavaOptions(String str) {
    Map<String, String> options = new HashMap<String, String>();
    String[] tokens = str.trim().split("\\s");
    for (String token : tokens) {
      if (token.isEmpty()) {
        continue;
      }
      if (!token.startsWith("-D")) {
        throw new IllegalArgumentException(
            "Cannot parse java option string [" + str + "]. Some options does not begin with -D prefix.");
      }
      String[] parts = token.substring(2).split("=", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException(
            "Cannot parse java option string [" + str + "]. The part [" + token + "] does not contain a =.");
      }

      options.put(parts[0], parts[1]);
    }
    return options;
  }

  public static String combineCsvLines(String[] lines) {
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(line).append("\n");
    }
    return sb.toString().trim();
  }

  public static String createCsvLine(String... parts) {
    StringBuilder sb = new StringBuilder();
    String quotes = "\"";
    String comma = ",";
    for (int i = 0; i < parts.length; i++) {
      sb.append(quotes).append(parts[i].replaceAll(quotes, quotes + quotes)).append(quotes);
      if (i != parts.length - 1) {
        sb.append(comma);
      }
    }
    return sb.toString();
  }

  public static String[][] parseCsvLines(String data) {
    if (data.isEmpty()) {
      return new String[0][];
    }
    String[] lines = data.split("\n");
    String[][] result = new String[lines.length][];
    for (int i = 0; i < lines.length; i++) {
      result[i] = parseCsvLine(lines[i]);
    }
    return result;
  }

  public static String[] parseCsvLine(String line) {
    List<String> store = new ArrayList<String>();
    StringBuilder curVal = new StringBuilder();
    boolean inquotes = false;
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (inquotes) {
        if (ch == '\"') {
          inquotes = false;
        } else {
          curVal.append(ch);
        }
      } else {
        if (ch == '\"') {
          inquotes = true;
          if (curVal.length() > 0) {
            //if this is the second quote in a value, add a quote
            //this is for the double quote in the middle of a value
            curVal.append('\"');
          }
        } else if (ch == ',') {
          store.add(curVal.toString());
          curVal = new StringBuilder();
        } else {
          curVal.append(ch);
        }
      }
    }
    store.add(curVal.toString());
    return store.toArray(new String[store.size()]);
  }
}
