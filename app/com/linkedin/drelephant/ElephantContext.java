package com.linkedin.drelephant;

import com.google.common.collect.Sets;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.util.HeuristicConfiguration;
import com.linkedin.drelephant.util.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static final ElephantContext INSTANCE = new ElephantContext();
  private static final String CONFIGURATION_FILE_PATH = "elephant-conf.xml";
  private static final String OPT_METRICS_PUB_CONF = "metrics.publisher-conf";

  private final List<String> _heuristicNames = new ArrayList<String>();
  private List<HeuristicConfigurationData> _heuristicsConfData;

  private final Map<String, ApplicationType> _nameToType = new HashMap<String, ApplicationType>();
  private final Map<ApplicationType, List<Heuristic>> _typeToHeuristics =
      new HashMap<ApplicationType, List<Heuristic>>();
  private final Map<ApplicationType, ElephantFetcher> _typeToFetcher = new HashMap<ApplicationType, ElephantFetcher>();

  private final DaliMetricsAPI.MetricsPublisher _metricsPublisher;
  private final String _hadoopVersion;

  private ElephantContext() {
    // private on purpose
    _hadoopVersion = Utils.getHadoopVersion();

    loadConfiguration();

    // Load metrics publisher
    // The getFile() API of Play returns a File object whether or not the actual file exists.
    String metricsPublisherConfPath = play.Play.application().configuration().getString(OPT_METRICS_PUB_CONF);
    if (metricsPublisherConfPath == null) {
      logger.info("Metrics publisher not configured. No metrics will be published");
      _metricsPublisher = null;
    } else {
      _metricsPublisher = DaliMetricsAPI.HDFSMetricsPublisher.createFromXml(metricsPublisherConfPath);
      if (_metricsPublisher == null) {
        logger.info("No metrics will be published");
      }
    }
  }

  private void loadConfiguration() {
    Document document = Utils.loadXMLDoc(CONFIGURATION_FILE_PATH);
    NodeList nodes = document.getDocumentElement().getChildNodes();

    Element heuristicsElement = null;
    Element fetchersElement = null;

    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        Element element = (Element) node;
        String sectionName = element.getTagName();
        if (sectionName.equals("heuristics")) {
          heuristicsElement = element;
        } else if (sectionName.equals("fetchers")) {
          fetchersElement = element;
        }
      }
    }

    if (fetchersElement == null) {
      throw new RuntimeException(
          "No <fetchers/> configuration block is presented in configuration file: " + CONFIGURATION_FILE_PATH);
    }
    if (heuristicsElement == null) {
      throw new RuntimeException(
          "No <heuristics/> configuration block is presented in configuration file: " + CONFIGURATION_FILE_PATH);
    }

    // It is important to load fetchers first because we also need to figure out the supported application
    // types via fetcher configurations.
    loadFetchers(fetchersElement);
    loadHeuristics(heuristicsElement);
    configureSupportedApplicationTypes();
  }

  private void loadFetchers(Element configuration) {
    NodeList nodes = configuration.getChildNodes();
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      int n = 0;
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        n++;
        Element fetcherNode = (Element) node;

        Node hadoopVersionNode = fetcherNode.getElementsByTagName("hadoopversion").item(0);
        if (hadoopVersionNode == null) {
          throw new RuntimeException("No hadoopversion tag presented in fetcher #" + n);
        }

        Node applicationTypeNode = fetcherNode.getElementsByTagName("applicationtype").item(0);
        if (applicationTypeNode == null) {
          throw new RuntimeException("No applicationtype tag presented in fetcher #" + n);
        }

        Node classNameNode = fetcherNode.getElementsByTagName("classname").item(0);
        if (classNameNode == null) {
          throw new RuntimeException("No classname tag presented in fetcher #" + n);
        }

        String hadoopVersion = hadoopVersionNode.getTextContent().toLowerCase().trim();
        if (hadoopVersion.equals(_hadoopVersion)) {
          String typeName = applicationTypeNode.getTextContent();
          if (!isApplicationTypeSupported(typeName)) {
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
              + "] does not match our current version [" + _hadoopVersion + "]");
        }
      }
    }
  }

  private void loadHeuristics(Element configuration) {
    _heuristicsConfData = new HeuristicConfiguration(configuration).getHeuristicsConfigurationData();

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
        new HeuristicConfigurationData(HeuristicResult.NO_DATA.getAnalysis(), null, "views.html.helpNoData", null));
  }

  private void configureSupportedApplicationTypes() {
    Set<ApplicationType> supportedTypes = Sets.intersection(_typeToFetcher.keySet(), _typeToHeuristics.keySet());
    for (ApplicationType type : _typeToFetcher.keySet()) {
      if (!supportedTypes.contains(type)) {
        ElephantFetcher removedFetcher = _typeToFetcher.remove(type);
        logger.warn("ElephantFetcher class " + removedFetcher.getClass().getName()
            + " does not have any heuristic rule for application type " + type.getName() + ", being ignored.");
      }
    }

    for (ApplicationType type : _typeToHeuristics.keySet()) {
      if (!supportedTypes.contains(type)) {
        List<Heuristic> removedHeuristics = _typeToHeuristics.remove(type);
        for (Heuristic removedHeuristic : removedHeuristics) {
          logger.warn("Heuristic class " + removedHeuristic.getClass().getName()
              + "does not have any fetcher for application type " + type.getName() + ", being ignored.");
        }
      }
    }

    logger.info("ElephantContext configured:");
    for (ApplicationType type : supportedTypes) {
      _nameToType.put(type.getName(), type);
      List<String> classes = new ArrayList<String>();
      List<Heuristic> heuristics = _typeToHeuristics.get(type);
      for (Heuristic heuristic : heuristics) {
        classes.add(heuristics.getClass().getName());
      }
      logger.info("ApplicationType: " + type.getName() + ", ElephantFetcher class: " + _typeToFetcher.get(type) +
          "Heuristics: [" + StringUtils.join(classes, ", ") + "]");
    }
  }

  /**
   * Get the singleton instance
   *
   * @return the ElephantContext singleton
   */
  public static ElephantContext instance() {
    return INSTANCE;
  }

  /**
   * Given an application type, return the currently bound heuristics
   *
   * @param type The application type
   * @return The corresponding heuristics
   */
  public List<Heuristic> getHeuristics(ApplicationType type) {
    return _typeToHeuristics.get(type);
  }

  /**
   * Given an application type, return the all the heuristic names.
   *
   * @return A list of heuristic names
   */
  public List<String> getAllHeuristicNames() {
    if (_heuristicNames.isEmpty()) {
      for (List<Heuristic> list : _typeToHeuristics.values()) {
        for (Heuristic heuristic : list) {
          _heuristicNames.add(heuristic.getHeuristicName());
        }
      }
    }

    return _heuristicNames;
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
  public ElephantFetcher getFetcher(ApplicationType type) {
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
  public ApplicationType getApplicationType(String typeName) {
    return _nameToType.get(typeName.toUpperCase());
  }

  /**
   * Indicate if an application type is supported by the current Dr. Elephant context
   *
   * @param typeName The type name to look for
   * @return true if supported else false
   */
  public boolean isApplicationTypeSupported(String typeName) {
    return _nameToType.containsKey(typeName.toUpperCase());
  }
}
