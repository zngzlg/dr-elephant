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

import com.linkedin.drelephant.schedulers.AzkabanScheduler;
import com.linkedin.drelephant.schedulers.Scheduler;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class InfoExtractorTest {

  @Test
  public void testGetSchedulerInstanceAzkaban() {
    Properties properties = new Properties();
    properties.put(AzkabanScheduler.AZKABAN_JOB_URL,
        "https://host:9000/manager?project=project-name&flow=flow-name&job=job-name");

    Scheduler scheduler = InfoExtractor.getSchedulerInstance("id", properties);
    assertEquals(true, scheduler instanceof AzkabanScheduler);
    assertEquals("https://host:9000/manager?project=project-name&flow=flow-name&job=job-name", scheduler.getJobDefId());
    assertEquals("azkaban", scheduler.getSchedulerName());
  }

  @Test
  public void testGetSchedulerInstanceNull() {
    Properties properties = new Properties();

    Scheduler scheduler = InfoExtractor.getSchedulerInstance("id", properties);
    assertEquals(null, scheduler);
  }

}
