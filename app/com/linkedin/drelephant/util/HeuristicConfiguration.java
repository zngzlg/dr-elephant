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

import com.linkedin.drelephant.analysis.ApplicationType;
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

        HeuristicConfigurationData heuristicData = new HeuristicConfigurationData(heuristicName, className, viewName,
            appType);
        _heuristicsConfDataList.add(heuristicData);

      }
    }
  }
}
