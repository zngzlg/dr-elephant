package com.linkedin.drelephant.analysis;

import java.util.HashMap;
import java.util.Map;


/**
 * Manages and represents supported application types.
 *
 * @author yizhou
 */
public class ApplicationType {
  private final String _name;

  private ApplicationType(String name) {
    _name = name.toUpperCase();
  }

  @Override
  public int hashCode() {
    return _name.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ApplicationType) {
      return ((ApplicationType) other).getName().equals(getName());
    }
    return false;
  }

  /**
   * Get the name
   *
   * @return the name of the application type
   */
  public String getName() {
    return _name;
  }

  private static final Map<String, ApplicationType> VALUES = new HashMap<String, ApplicationType>();

  /**
   * Register an application type into the supported map.
   *
   * @param typeName The type name
   */
  public static void addType(String typeName) {
    if (typeName == null) {
      return;
    }

    ApplicationType type = new ApplicationType(typeName);
    VALUES.put(type.getName(), type);
  }

  /**
   * Get the corresponding application type instance via type name
   * @param typeName The type name
   * @return the application type, null if not found
   */
  public static ApplicationType getType(String typeName) {
    if (typeName == null) {
      return null;
    }
    return VALUES.get(typeName.toUpperCase());
  }

  /**
   * Indicate if a particular application type is supported.
   *
   * @param typeName The type name
   * @return true if supported else false
   */
  public static boolean isSupported(String typeName) {
    ApplicationType type = getType(typeName);

    if (type == null) {
      return false;
    }

    return true;
  }
}
