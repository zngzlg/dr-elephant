/*
 * Copyright 2016 LinkedIn Corp.
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

package com.linkedin.drelephant.configurations.fetcher;

import com.linkedin.drelephant.analysis.ApplicationType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * This class manages the Fetcher Configurations
 */
public class FetcherConfiguration {
  private static final Logger logger = Logger.getLogger(FetcherConfiguration.class);
  private List<FetcherConfigurationData> _fetchersConfDataList;

  public FetcherConfiguration(Element configuration) {
    parseFetcherConfiguration(configuration);
  }

  /**
   * Returns the list of Fetchers along with their Configuration Information
   *
   * @return A list of Configuration Data for the fetchers
   */
  public List<FetcherConfigurationData> getFetchersConfigurationData() {
    return _fetchersConfDataList;
  }

  /**
   * Parses the Fetcher configuration file and loads the Fetcher Information to a list of FetcherConfigurationData
   *
   * @param configuration The dom Element to be parsed
   */
  private void parseFetcherConfiguration(Element configuration) {
    _fetchersConfDataList = new ArrayList<FetcherConfigurationData>();

    NodeList nodes = configuration.getChildNodes();
    int n = 0;
    for (int i = 0; i < nodes.getLength(); i++) {
      // Each heuristic node
      Node node = nodes.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        n++;
        Element fetcherNode = (Element) node;

        String className;
        Node classNameNode = fetcherNode.getElementsByTagName("classname").item(0);
        if (classNameNode == null) {
          throw new RuntimeException("No tag 'classname' in fetcher " + n);
        }
        className = classNameNode.getTextContent();
        if (className.equals("")) {
          throw new RuntimeException("Empty tag 'classname' in fetcher " + n);
        }

        Node appTypeNode = fetcherNode.getElementsByTagName("applicationtype").item(0);
        if (appTypeNode == null) {
          throw new RuntimeException(
              "No tag or invalid tag 'applicationtype' in fetcher " + n + " classname " + className);
        }
        String appTypeStr = appTypeNode.getTextContent();
        if (appTypeStr == null) {
          logger.error("Application type is not specified in fetcher " + n + " classname " + className
              + ". Skipping this configuration.");
          continue;
        }
        ApplicationType appType = new ApplicationType(appTypeStr);

        // Check if parameters are defined for the heuristic
        Map<String, String> paramsMap = new HashMap<String, String>();
        Node paramsNode = fetcherNode.getElementsByTagName("params").item(0);
        if (paramsNode != null) {
          NodeList paramsList = paramsNode.getChildNodes();
          for (int j = 0; j < paramsList.getLength(); j++) {
            Node paramNode = paramsList.item(j);
            if (paramNode != null && !paramsMap.containsKey(paramNode.getNodeName())) {
              paramsMap.put(paramNode.getNodeName(), paramNode.getTextContent());
            }
          }
        }

        FetcherConfigurationData fetcherData = new FetcherConfigurationData(className, appType, paramsMap);
        _fetchersConfDataList.add(fetcherData);

      }
    }
  }

}
