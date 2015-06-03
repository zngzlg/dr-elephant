package com.linkedin.drelephant.util;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopSystemContext;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class HeuristicConfiguration {
  private static final Logger logger = Logger.getLogger(HeuristicConfiguration.class);
  private List<HeuristicConfigurationData> _heuristicsConfDataList;

  public HeuristicConfiguration(Element configuration) {
    parseHeuristicConfiguration(configuration);
  }

  public List<HeuristicConfigurationData> getHeuristicsConfigurationData() {
    return _heuristicsConfDataList;
  }

  private void parseHeuristicConfiguration(Element configuration) {
    int hadoopVersion = HadoopSystemContext.getHadoopVersion();

    _heuristicsConfDataList = new ArrayList<HeuristicConfigurationData>();

    NodeList nodes = configuration.getChildNodes();
    int n = 0;
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        n++;
        Element heuristicNode = (Element) node;

        String className;
        Node classNameNode = heuristicNode.getElementsByTagName("classname").item(0);
        if (classNameNode == null) {
          throw new RuntimeException("No tag 'classname' in heuristic " + n);
        }
        className = classNameNode.getTextContent();
        if (className.equals("")) {
          throw new RuntimeException("Empty tag 'classname' in heuristic " + n);
        }

        String heuristicName;
        Node heuristicNameNode = heuristicNode.getElementsByTagName("heuristicname").item(0);
        if (heuristicNameNode == null) {
          throw new RuntimeException("No tag 'heuristicname' in heuristic " + n + " classname " + className);
        }
        heuristicName = heuristicNameNode.getTextContent();
        if (heuristicName.equals("")) {
          throw new RuntimeException("Empty tag 'heuristicname' in heuristic " + n + " classname " + className);
        }

        String viewName;
        Node viewNameNode = heuristicNode.getElementsByTagName("viewname").item(0);
        if (viewNameNode == null) {
          throw new RuntimeException("No tag 'viewname' in heuristic " + n + " classname " + className);
        }
        viewName = viewNameNode.getTextContent();
        if (viewName.equals("")) {
          throw new RuntimeException("Empty tag 'viewname' in heuristic " + n + " classname " + className);
        }

        Node versionsNode = heuristicNode.getElementsByTagName("hadoopversions").item(0);
        if (versionsNode == null || versionsNode.getNodeType() != Node.ELEMENT_NODE) {
          throw new RuntimeException(
              "No tag or invalid tag 'hadoopversions' in heuristic " + n + " classname " + className);
        }
        NodeList versionList = ((Element) versionsNode).getElementsByTagName("version");

        Node appTypeNode = heuristicNode.getElementsByTagName("applicationtype").item(0);
        if (appTypeNode == null) {
          throw new RuntimeException(
              "No tag or invalid tag 'applicationtype' in heuristic " + n + " classname " + className);
        }
        String appTypeStr = appTypeNode.getTextContent();
        if (appTypeStr == null) {
          logger.error("Application type is not specified in heuristic " + n + " classname " + className
                  + ". Skipping this configuration.");
          continue;
        }
        ApplicationType appType = new ApplicationType(appTypeStr);

        for (int j = 0; j < versionList.getLength(); j++) {
          String version = versionList.item(j).getTextContent();
          int majorVersion = Utils.getMajorVersionFromString(version);
          if (hadoopVersion == majorVersion) {
            HeuristicConfigurationData heuristicData =
                new HeuristicConfigurationData(heuristicName, className, viewName, appType);
            _heuristicsConfDataList.add(heuristicData);
            break;
          } else {
            logger.warn(
                "Ignoring Heuristic: " + heuristicName + " className: " + className + "because it's hadoop version: "
                    + majorVersion + " does not match the current hadoop version number " + hadoopVersion);
          }
        }
      }
    }
  }
}
