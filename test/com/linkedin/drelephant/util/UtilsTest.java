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
import java.util.HashMap;
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

  public void testGetParam() {
    Map<String, String> paramMap = new HashMap<String, String>();
    paramMap.put("test_severity_1", "10, 50, 100, 200");
    paramMap.put("test_severity_2", "2, 4, 8");
    paramMap.put("test_param_1", "2!");
    paramMap.put("test_param_2", "2");

    double limits1[] = Utils.getParam(paramMap.get("test_severity_1"), 4);
    assertEquals(10d, limits1[0]);
    assertEquals(50d, limits1[1]);
    assertEquals(100d, limits1[2]);
    assertEquals(200d, limits1[3]);

    double limits2[] = Utils.getParam(paramMap.get("test_severity_2"), 4);
    assertEquals(null, limits2);

    double limits3[] = Utils.getParam(paramMap.get("test_param_1"), 1);
    assertEquals(null, limits3);

    double limits4[] = Utils.getParam(paramMap.get("test_param_2"), 1);
    assertEquals(2d, limits4[0]);
  }

}
