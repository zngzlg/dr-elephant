package com.linkedin.drelephant.analysis;

import java.util.Properties;
import java.util.regex.Pattern;


public class JobType {
  private final String _name;
  private final String _confName;
  private final Pattern _confPattern;

  /**
   * Constructor for a JobType
   *
   * @param name The name of the job type
   * @param confName The configuration to look into
   * @param confPattern The regex pattern to match the configuration property
   */
  public JobType(String name, String confName, String confPattern) {
    _name = name;
    _confName = confName;
    _confPattern = Pattern.compile(confPattern);
  }

  /**
   * Check if a JobType matches a property
   *
   * @param jobProp The properties to match
   * @return true if matched else false
   */
  public boolean matchType(Properties jobProp) {
    // Always return false if confName/confPattern is undefined,
    // which means we cannot tell if the properties are matching the pattern
    if (_confName == null || _confPattern == null) {
      return false;
    }

    return jobProp.containsKey(_confName) && _confPattern.matcher((String) jobProp.get(_confName)).matches();
  }

  /**
   * Get the name of the job type
   *
   * @return The name
   */
  public String getName() {
    return _name;
  }

  @Override
  public String toString() {
    return getName();
  }
}
