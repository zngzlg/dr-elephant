package com.linkedin.drelephant.util;

import com.linkedin.drelephant.analysis.ApplicationType;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class HeuristicConf {
  private static final Logger logger = Logger.getLogger(HeuristicConf.class);
  private List<HeuristicConfData> _heuristicsConfDataList;

  public HeuristicConf(Element configuration) {
    parseHeuristicConf(configuration);
  }

  public List<HeuristicConfData> getHeuristicsConfData() {
    return _heuristicsConfDataList;
  }

  private void parseHeuristicConf(Element configuration) {
    String hadoopVersion = Utils.getHadoopVersion();

    _heuristicsConfDataList = new ArrayList<HeuristicConfData>();

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
        ApplicationType appType = ApplicationType.getType(appTypeStr);
        if (appType == null) {
          logger.error(
              "[" + appTypeStr + "] is not a valid application type in heuristic " + n + " classname " + className
                  + ". Skipping this configuration.");
          continue;
        }

        for (int j = 0; j < versionList.getLength(); j++) {
          String version = versionList.item(j).getTextContent();
          if (version.equals(hadoopVersion)) {
            HeuristicConfData heuristicData = new HeuristicConfData(heuristicName, className, viewName, appType);
            _heuristicsConfDataList.add(heuristicData);
            break;
          }
        }
      }
    }
  }
}
