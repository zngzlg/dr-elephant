package com.linkedin.drelephant.analysis;

/**
 * Manages and represents supported application types.
 *
 * @author yizhou
 */
public class ApplicationType {
  private final String _name;

  public ApplicationType(String name) {
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
}
