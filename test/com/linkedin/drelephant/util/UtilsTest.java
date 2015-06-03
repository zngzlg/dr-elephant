package com.linkedin.drelephant.util;

import java.util.Map;
import junit.framework.TestCase;


/**
 * This class tests the Utils class
 *
 */
public class UtilsTest extends TestCase {
  public void testParseJavaOptions() {
    Map<String, String> options1 = Utils.parseJavaOptions("-Dfoo=bar");
    assertEquals(1, options1.size());
    assertEquals("bar", options1.get("foo"));

    Map<String, String> options2 = Utils.parseJavaOptions(" -Dfoo=bar   -Dfoo2=bar2 -Dfoo3=bar3");
    assertEquals(3, options2.size());
    assertEquals("bar", options2.get("foo"));
    assertEquals("bar2", options2.get("foo2"));
    assertEquals("bar3", options2.get("foo3"));
  }

  public void testGetMajorVersionFromString() {
    assertEquals(1, Utils.getMajorVersionFromString("1"));
    assertEquals(2, Utils.getMajorVersionFromString("2.1"));
    assertEquals(2, Utils.getMajorVersionFromString("2.1.3"));
    assertEquals(2, Utils.getMajorVersionFromString("2.1.3-LI0"));

    // Negative cases
    testGetMajorVersionFromStringNegative(null);
    testGetMajorVersionFromStringNegative(".1");
    testGetMajorVersionFromStringNegative("-LI0");
    testGetMajorVersionFromStringNegative("a");
    testGetMajorVersionFromStringNegative("1.a");
  }

  private void testGetMajorVersionFromStringNegative(String input){
    // Expecting IllegalArgumentException
    try {
      Utils.getMajorVersionFromString(input);
      fail();
    } catch (IllegalArgumentException e) {
      // The exception is expected, do nothing
    }
  }
}
