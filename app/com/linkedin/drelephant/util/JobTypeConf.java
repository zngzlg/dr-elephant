package com.linkedin.drelephant.util;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.PatternSyntaxException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.linkedin.drelephant.analysis.JobType;

import play.Play;


public class JobTypeConf {
  private static final Logger logger = Logger.getLogger(JobTypeConf.class);
  private static final int TYPE_LEN_LIMIT = 20;
  // This is a default job type we will add into the type list even not configured.
  private static final String DEFAULT_TYPE = "HadoopJava";
  private static final String CONFIG_FILE_PATH = "job-types.xml";
  private static JobTypeConf _heuristicConfInstance = new JobTypeConf();

  private List<JobType> _jobTypeList;

  private JobTypeConf() {
    parseJobTypeConf();
  }

  public static JobTypeConf instance() {
    return _heuristicConfInstance;
  }

  public List<JobType> getJobTypeList() {
    return _jobTypeList;
  }

  public JobType getJobType(HadoopApplicationData data) {
    Properties conf = data.getConf();
    for (JobType type : _jobTypeList) {
      if (type.matchType(conf)) {
        return type;
      }
    }
    return JobType.NO_DATA;
  }

  private void parseJobTypeConf() {

    logger.info("Loading job type config file " + CONFIG_FILE_PATH);
    _jobTypeList = new ArrayList<JobType>();

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
        Element jobTypeNode = (Element) node;
        String jobTypeName;
        Node jobTypeNameNode = jobTypeNode.getElementsByTagName("type").item(0);
        if (jobTypeNameNode == null) {
          throw new RuntimeException("No tag 'jobtype' in jobtype " + n);
        }
        jobTypeName = jobTypeNameNode.getTextContent();
        if (jobTypeName.equals("")) {
          throw new RuntimeException("Empty tag 'jobtype' in jobtype " + n);
        }
        // Truncate jobtype length for db constraint
        if (jobTypeName.length() > TYPE_LEN_LIMIT) {
          logger.info("Truncate type " + jobTypeName.length());
          jobTypeName = jobTypeName.substring(0, TYPE_LEN_LIMIT);
        }

        String jobConfName;
        Node jobConfNameNode = jobTypeNode.getElementsByTagName("conf").item(0);
        if (jobConfNameNode == null) {
          throw new RuntimeException("No tag 'conf' in jobtype " + jobTypeName );
        }
        jobConfName = jobConfNameNode.getTextContent();
        if (jobConfName.equals("")) {
          throw new RuntimeException("Empty tag 'conf' in jobtype " + jobTypeName);
        }

        String jobConfValue;
        Node jobConfValueNode = jobTypeNode.getElementsByTagName("value").item(0);
        if (jobConfValueNode == null) {
          // Default regex. match any char one or more times
          jobConfValue = ".*";
        } else {
          jobConfValue = jobConfValueNode.getTextContent();
          if (jobConfValue.equals("")) {
            jobConfValue = ".*";
          }
        }
        logger.info("Loaded jobType:" + jobTypeName + " confName:" + jobConfName + " confValue:" + jobConfValue);
        try {
          _jobTypeList.add(new JobType(jobTypeName, jobConfName, jobConfValue));
        } catch (PatternSyntaxException e) {
          throw new RuntimeException("Error processing this pattern.  Pattern:" + jobConfValue + " jobtype:" + jobTypeName);
        }
      }
    }
    // Add default type to the list. All jobs will match this type because this conf always exists
    if (Utils.getHadoopVersion().equals("yarn")) {
      _jobTypeList.add(new JobType(DEFAULT_TYPE, "mapreduce.framework.name", ".*"));
    } else {
      _jobTypeList.add(new JobType(DEFAULT_TYPE, "mapred.job.tracker", ".*"));
    }
    logger.info("Loaded total " + _jobTypeList.size() + " job types.");
  }
}
