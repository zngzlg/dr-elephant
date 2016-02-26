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

package com.linkedin.drelephant.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import play.Play;


/**
 * This class contains all the utility methods.
 */
public final class Utils {
  private static final Logger logger = Logger.getLogger(Utils.class);
  // Matching x.x.x or x.x.x-li1 (x are numbers)
  public static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)(?:\\.\\d+)*(?:\\-[\\dA-Za-z]+)?");

  private Utils() {
    // do nothing
  }

  /**
   * Given a mapreduce job's application id, get its corresponding job id
   *
   * Note: before adding Spark analysers, all JobResult were using job ids as the primary key. But Spark and many
   * other non-mapreduce applications do not have a job id. To maintain backwards compatibility, we replace
   * 'application' with 'job' to form a pseudo job id.
   *
   * @param appId The application id of the job
   * @return the corresponding job id
   */
  public static String getJobIdFromApplicationId(String appId) {
    return appId.replaceAll("application", "job");
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

  /**
   * Returns the configured thresholds after evaluating and verifying the levels.
   *
   * @param rawLimits A comma separated string of threshold limits
   * @param thresholdLevels The number of threshold levels
   * @return The evaluated threshold limits
   */
  public static double[] getParam(String rawLimits, int thresholdLevels) {
    double[] parsedLimits = new double[thresholdLevels];

    if (rawLimits != null) {
      String[] thresholds = rawLimits.split(",");
      if (thresholds.length != thresholdLevels) {
        logger.error("Could not find " + thresholdLevels + " threshold levels in "  + rawLimits);
        parsedLimits = null;
      } else {
        // Evaluate the limits
        ScriptEngineManager mgr = new ScriptEngineManager();
        ScriptEngine engine = mgr.getEngineByName("JavaScript");
        for (int i = 0; i < thresholdLevels; i++) {
          try {
            parsedLimits[i] = Double.parseDouble(engine.eval(thresholds[i]).toString());
          } catch (ScriptException e) {
            logger.error("Could not evaluate " + thresholds[i] + " in " + rawLimits);
            parsedLimits = null;
          }
        }
      }
    }

    return parsedLimits;
  }

}
