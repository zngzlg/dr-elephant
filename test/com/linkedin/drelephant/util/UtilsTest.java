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
}
