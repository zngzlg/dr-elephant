package com.linkedin.drelephant.util;

import com.linkedin.drelephant.analysis.ApplicationType;


public class HeuristicConfData {
  private final String _heuristicName;
  private final String _className;
  private final String _viewName;
  private final ApplicationType _appType;

  public HeuristicConfData(String heuristicName, String className, String viewName, ApplicationType appType) {
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
