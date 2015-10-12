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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.HadoopSystemContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.JobType;
import com.linkedin.drelephant.util.HeuristicConfiguration;
import com.linkedin.drelephant.util.HeuristicConfigurationData;
import com.linkedin.drelephant.util.JobTypeConf;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import play.api.Play;


/**
 * This is a general singleton instance that provides globally accessible resources.
 *
 * It is not mandatory that an AnalysisPromise implementation must leverage this instance, but this context provides
 * a way for Promises to access shared objects (singletons, thread-local variables and etc.).
 */
public class ElephantContext {
  private static final Logger logger = Logger.getLogger(ElephantContext.class);
  private static ElephantContext INSTANCE;

  private static final String HADOOP_VERSION_XML_FIELD = "hadoopversion";
  private static final String CLASS_NAME_XML_FIELD = "classname";
  private static final String APPLICATION_TYPE_XML_FIELD = "applicationtype";

  private static final String FETCHERS_CONF = "FetcherConf.xml";
  private static final String HEURISTICS_CONF = "HeuristicConf.xml";
  private static final String JOB_TYPES_CONF = "JobTypeConf.xml";
  private static final String OPT_METRICS_PUB_CONF = "CounterPublisherConf.xml";

  private final Map<String, List<String>> _heuristicGroupedNames = new HashMap<String, List<String>>();
  private List<HeuristicConfigurationData> _heuristicsConfData;

  private final Map<String, ApplicationType> _nameToType = new HashMap<String, ApplicationType>();
  private final Map<ApplicationType, List<Heuristic>> _typeToHeuristics =
      new HashMap<ApplicationType, List<Heuristic>>();
  private final Map<ApplicationType, ElephantFetcher> _typeToFetcher = new HashMap<ApplicationType, ElephantFetcher>();
  private Map<ApplicationType, List<JobType>> _appTypeToJobTypes = new HashMap<ApplicationType, List<JobType>>();

  private final DaliMetricsAPI.MetricsPublisher _metricsPublisher;

  public static void init() {
    INSTANCE = new ElephantContext();
  }

  public static ElephantContext instance() {
    if (INSTANCE == null) {
      INSTANCE = new ElephantContext();
    }
    return INSTANCE;
  }

  // private on purpose
  private ElephantContext() {
    loadConfiguration();

    // Load metrics publisher
    _metricsPublisher = DaliMetricsAPI.HDFSMetricsPublisher.createFromXml(OPT_METRICS_PUB_CONF);
    if (_metricsPublisher == null) {
      logger.info("No metrics will be published. ");
    } else {
      logger.info("Metrics publisher configured. ");
    }
  }

  private void loadConfiguration() {
    loadFetchers();
    loadHeuristics();
    loadJobTypes();

    // It is important to configure supported types in the LAST step so that we could have information from all
    // configurable components.
    configureSupportedApplicationTypes();
  }

  private void loadFetchers() {

    Document document = Utils.loadXMLDoc(FETCHERS_CONF);

    NodeList nodes = document.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      int n = 0;
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        n++;
        Element fetcherNode = (Element) node;

        Node hadoopVersionNode = fetcherNode.getElementsByTagName(HADOOP_VERSION_XML_FIELD).item(0);
        if (hadoopVersionNode == null) {
          throw new RuntimeException("No hadoopversion tag presented in fetcher #" + n);
        }

        Node applicationTypeNode = fetcherNode.getElementsByTagName(APPLICATION_TYPE_XML_FIELD).item(0);
        if (applicationTypeNode == null) {
          throw new RuntimeException("No applicationtype tag presented in fetcher #" + n);
        }

        Node classNameNode = fetcherNode.getElementsByTagName(CLASS_NAME_XML_FIELD).item(0);
        if (classNameNode == null) {
          throw new RuntimeException("No classname tag presented in fetcher #" + n);
        }

        String hadoopVersion = hadoopVersionNode.getTextContent().toLowerCase().trim();
        int hadoopMajorVersion = Utils.getMajorVersionFromString(hadoopVersion);
        if (HadoopSystemContext.matchCurrentHadoopVersion(hadoopMajorVersion)) {
          String typeName = applicationTypeNode.getTextContent();
          if (getApplicationTypeForName(typeName) == null) {
            ApplicationType type = new ApplicationType(typeName);

            String className = classNameNode.getTextContent();
            try {
              Class<?> fetcherClass = Play.current().classloader().loadClass(className);
              Object instance = fetcherClass.newInstance();
              if (!(instance instanceof ElephantFetcher)) {
                throw new IllegalArgumentException(
                    "Class " + fetcherClass.getName() + " is not an implementation of " + ElephantFetcher.class
                        .getName());
              }
              _typeToFetcher.put(type, (ElephantFetcher) fetcherClass.newInstance());
            } catch (ClassNotFoundException e) {
              throw new RuntimeException("Class" + className + " not found for fetcher #" + n, e);
            } catch (InstantiationException e) {
              throw new RuntimeException("Could not instantiate class " + className, e);
            } catch (IllegalAccessException e) {
              throw new RuntimeException("Could not access constructor for class " + className, e);
            }
          } else {
            throw new RuntimeException(
                "Given a hadoop version and an application type, there could only be one fetcher. Fetcher #" + n
                    + " is duplicated with the previous fetchers.");
          }
        } else {
          logger.info("Skipping fetcher #" + n + ", because its hadoop version [" + hadoopVersion
              + "] does not match our current Hadoop version.");
        }
      }
    }
  }

  private void loadHeuristics() {

    Document document = Utils.loadXMLDoc(HEURISTICS_CONF);

    _heuristicsConfData = new HeuristicConfiguration(document.getDocumentElement()).getHeuristicsConfigurationData();

    for (HeuristicConfigurationData data : _heuristicsConfData) {
      try {
        Class<?> heuristicClass = Play.current().classloader().loadClass(data.getClassName());
        Object instance = heuristicClass.newInstance();
        if (!(instance instanceof Heuristic)) {
          throw new IllegalArgumentException(
              "Class " + heuristicClass.getName() + " is not an implementation of " + Heuristic.class.getName());
        }
        Heuristic heuristicInstance = (Heuristic) heuristicClass.newInstance();

        ApplicationType type = data.getAppType();
        List<Heuristic> heuristics = _typeToHeuristics.get(type);
        if (heuristics == null) {
          heuristics = new ArrayList<Heuristic>();
          _typeToHeuristics.put(type, heuristics);
        }
        heuristics.add(heuristicInstance);

        logger.info("Load Heuristic : " + data.getClassName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not find class " + data.getClassName(), e);
      } catch (InstantiationException e) {
        throw new RuntimeException("Could not instantiate class " + data.getClassName(), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Could not access constructor for class" + data.getClassName(), e);
      } catch (RuntimeException e) {
        //More descriptive on other runtime exception such as ClassCastException
        throw new RuntimeException(data.getClassName() + " is not a valid Heuristic class.", e);
      }
    }

    // Bind No_DATA heuristic to its helper pages, no need to add any real configurations
    _heuristicsConfData.add(
        new HeuristicConfigurationData(HeuristicResult.NO_DATA.getAnalysis(), null, "views.html.help.helpNoData", null));
  }

  /**
   * Decides what application types can be supported.
   *
   * An application type is supported if all the below are true.
   * 1. A Fetcher is defined in FetcherConf.xml for the application type.
   * 2. At least one Heuristic is configured in HeuristicConf.xml for the application type.
   * 3. At least one job type is configured in JobTypeConf.xml for the application type.
   */
  private void configureSupportedApplicationTypes() {
    Set<ApplicationType> supportedTypes = Sets.intersection(_typeToFetcher.keySet(), _typeToHeuristics.keySet());
    supportedTypes = Sets.intersection(supportedTypes, _appTypeToJobTypes.keySet());

    _typeToFetcher.keySet().retainAll(supportedTypes);
    _typeToHeuristics.keySet().retainAll(supportedTypes);
    _appTypeToJobTypes.keySet().retainAll(supportedTypes);

    logger.info("ElephantContext configured:");
    for (ApplicationType type : supportedTypes) {
      _nameToType.put(type.getName(), type);

      List<String> classes = new ArrayList<String>();
      List<Heuristic> heuristics = _typeToHeuristics.get(type);
      for (Heuristic heuristic : heuristics) {
        classes.add(heuristic.getClass().getName());
      }

      List<JobType> jobTypes = _appTypeToJobTypes.get(type);
      logger.info("Supports " + type.getName() + " application type, using " + _typeToFetcher.get(type)
          + " fetcher class with Heuristics [" + StringUtils.join(classes, ", ") + "] and following JobTypes ["
          + StringUtils.join(jobTypes, ", ") + "].");
    }
  }

  private void loadJobTypes() {
    JobTypeConf conf = new JobTypeConf(JOB_TYPES_CONF);
    _appTypeToJobTypes = conf.getAppTypeToJobTypeList();
  }

  /**
   * Given an application type, return the currently bound heuristics
   *
   * @param type The application type
   * @return The corresponding heuristics
   */
  public List<Heuristic> getHeuristicsForApplicationType(ApplicationType type) {
    return _typeToHeuristics.get(type);
  }

  /**
   * Return the heuristic names available grouped by application type.
   *
   * @return A map of application type name -> a list of heuristic names
   */
  public Map<String, List<String>> getAllHeuristicNames() {
    if (_heuristicGroupedNames.isEmpty()) {
      for (Map.Entry<ApplicationType, List<Heuristic>> entry : _typeToHeuristics.entrySet()) {
        ApplicationType type = entry.getKey();
        List<Heuristic> list = entry.getValue();

        List<String> nameList = new ArrayList<String>();
        for (Heuristic heuristic : list) {
          nameList.add(heuristic.getHeuristicName());
        }

        Collections.sort(nameList);
        _heuristicGroupedNames.put(type.getName(), nameList);
      }
    }

    return _heuristicGroupedNames;
  }

  /**
   * Get the heuristic configuration data
   *
   * @return The configuration data of heuristics
   */
  public List<HeuristicConfigurationData> getHeuristicsConfigurationData() {
    return _heuristicsConfData;
  }

  /**
   * Given an application type, return the currently ElephantFetcher that binds with the type.
   *
   * @param type The application type
   * @return The corresponding fetcher
   */
  public ElephantFetcher getFetcherForApplicationType(ApplicationType type) {
    return _typeToFetcher.get(type);
  }

  /**
   * Get the DALI metrics publisher
   *
   * @return the DALI Metrics publisher
   */
  public DaliMetricsAPI.MetricsPublisher getMetricsPublisher() {
    return _metricsPublisher;
  }

  /**
   * Get the application type given a type name.
   *
   * @return The corresponding application type, null if not found
   */
  public ApplicationType getApplicationTypeForName(String typeName) {
    return _nameToType.get(typeName.toUpperCase());
  }

  /**
   * Get the matched job type given a
   *
   * @param data The HadoopApplicationData to check
   * @return The matched job type
   */
  public JobType matchJobType(HadoopApplicationData data) {
    if (data != null) {
      List<JobType> jobTypeList = _appTypeToJobTypes.get(data.getApplicationType());
      Properties jobProp = data.getConf();
      for (JobType type : jobTypeList) {
        if (type.matchType(jobProp)) {
          return type;
        }
      }
    }
    return null;
  }

  public Map<ApplicationType, List<JobType>> getAppTypeToJobTypes() {
    return ImmutableMap.copyOf(_appTypeToJobTypes);
  }
}
