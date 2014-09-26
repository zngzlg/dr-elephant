package com.linkedin.drelephant.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class HeuristicConf {

  private static final String FILE_PATH = "conf/HeuristicConf.xml";
  private List<HeuristicConfData> heuristicsConfData;
  private static HeuristicConf heuristicConfInstance = new HeuristicConf();

  private HeuristicConf() {
    parseHeuristicConf();
  }

  public static HeuristicConf instance() {
    return heuristicConfInstance;
  }

  public List<HeuristicConfData> getHeuristicsConfData() {
    return heuristicsConfData;
  }

  private void parseHeuristicConf() {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    heuristicsConfData = new ArrayList<HeuristicConfData>();
    DocumentBuilder builder;
    Document document = null;
    try {
      FileInputStream fstream = new FileInputStream(FILE_PATH);
      builder = factory.newDocumentBuilder();
      document = builder.parse(fstream);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Could not parse configuration file " + FILE_PATH
          + ". Check if the parser is property configured", e);
    } catch (SAXException e) {
      throw new RuntimeException("PARSE ERROR: Could not parse configuration file " + FILE_PATH
          + ". Check if the file is properly formed", e);
    } catch (IOException e) {
      throw new RuntimeException("I/O ERROR: Not able to read " + FILE_PATH
          + "  file. Check if the file exists and it has proper permissions.", e);
    }

    NodeList nodes = document.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      HeuristicConfData heuristicConfData = null;
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        Element heuristic = (Element) node;
        String className = heuristic.getElementsByTagName("classname").item(0).getTextContent();
        String heuristicName = heuristic.getElementsByTagName("heuristicname").item(0).getTextContent();
        String viewName = heuristic.getElementsByTagName("viewname").item(0).getTextContent();
        Node versionNode = heuristic.getElementsByTagName("hadoopversions").item(0);
        if (versionNode.getNodeType() == Node.ELEMENT_NODE) {
          ArrayList<String> hadoopVersions = new ArrayList<String>();
          Element versions = (Element) versionNode;
          NodeList versionList = versions.getElementsByTagName("version");
          for (int j = 0; j < versionList.getLength(); j++) {
            hadoopVersions.add(versionList.item(j).getTextContent());
          }
          heuristicConfData = new HeuristicConfData(heuristicName, className, viewName, hadoopVersions);
        }
        heuristicsConfData.add(heuristicConfData);
      }
    }
  }

}
