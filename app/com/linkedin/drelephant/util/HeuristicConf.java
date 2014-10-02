package com.linkedin.drelephant.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import play.Logger;
import play.Play;


public class HeuristicConf {

  private static final String CONFIG_FILE_PATH = "HeuristicConf.xml";
  private List<HeuristicConfData> _heuristicsConfDataList;
  private static HeuristicConf _heuristicConfInstance = new HeuristicConf();

  private HeuristicConf() {
    parseHeuristicConf();
  }

  public static HeuristicConf instance() {
    return _heuristicConfInstance;
  }

  public List<HeuristicConfData> getHeuristicsConfData() {
    return _heuristicsConfDataList;
  }

  private void parseHeuristicConf() {

    Configuration hadoopConf = new Configuration();
    String hadoopVersion = hadoopConf.get("mapreduce.framework.name");
    if (hadoopVersion == null) {
      if (hadoopConf.get("mapred.job.tracker.http.address") != null) {
        hadoopVersion = "classic";
      } else {
        throw new RuntimeException("Hadoop config error. No framework name provided.");
      }
    }

    Logger.info("Loading pluggable heuristics config file " + CONFIG_FILE_PATH);
    _heuristicsConfDataList = new ArrayList<HeuristicConfData>();

    InputStream instream = Play.application().resourceAsStream(CONFIG_FILE_PATH);
    if (instream == null) {
      throw new RuntimeException("File " + CONFIG_FILE_PATH + " does not exist.");
    }

    Document document = null;
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      document = builder.parse(instream);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("XML Parser could not be created.", e);
    } catch (SAXException e) {
      throw new RuntimeException(CONFIG_FILE_PATH + " is not properly formed", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read " + CONFIG_FILE_PATH, e);
    }

    NodeList nodes = document.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      int n = 0;
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
          throw new RuntimeException("No tag 'heuristicname' in heuristic " + n);
        }
        heuristicName = heuristicNameNode.getTextContent();
        if (heuristicName.equals("")) {
          throw new RuntimeException("Empty tag 'heuristicname' in heuristic " + n);
        }

        String viewName;
        Node viewNameNode = heuristicNode.getElementsByTagName("viewname").item(0);
        if (viewNameNode == null) {
          throw new RuntimeException("No tag 'viewname' in heuristic " + n);
        }
        viewName = viewNameNode.getTextContent();
        if (viewName.equals("")) {
          throw new RuntimeException("Empty tag 'viewname' in heuristic " + n);
        }

        Node versionsNode = heuristicNode.getElementsByTagName("hadoopversions").item(0);
        if (versionsNode == null || versionsNode.getNodeType() != Node.ELEMENT_NODE) {
          throw new RuntimeException("No tag or invalid tag 'hadoopversions' in heuristic " + n);
        }
        NodeList versionList = ((Element) versionsNode).getElementsByTagName("version");
        for (int j = 0; j < versionList.getLength(); j++) {
          String version = versionList.item(j).getTextContent();
          if (version.equals(hadoopVersion)) {
            HeuristicConfData heuristicData = new HeuristicConfData(heuristicName, className, viewName);
            _heuristicsConfDataList.add(heuristicData);
            break;
          }
        }
      }
    }
  }
}
