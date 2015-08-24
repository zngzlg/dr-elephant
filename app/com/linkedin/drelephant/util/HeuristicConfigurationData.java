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


public class HeuristicConfigurationData {
  private final String _heuristicName;
  private final String _className;
  private final String _viewName;
  private final ApplicationType _appType;

  public HeuristicConfigurationData(String heuristicName, String className, String viewName, ApplicationType appType) {
    _heuristicName = heuristicName;
    _className = className;
    _viewName = viewName;
    _appType = appType;
  }

  public String getHeuristicName() {
    return _heuristicName;
  }

  public String getClassName() {
    return _className;
  }

  public String getViewName() {
    return _viewName;
  }

  public ApplicationType getAppType() {
    return _appType;
  }
}
