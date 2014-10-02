package com.linkedin.drelephant.util;

public class HeuristicConfData {
  private String _heuristicName;
  private String _className;
  private String _viewName;

  public HeuristicConfData(String heuristicName, String className, String viewName) {
    this._heuristicName = heuristicName;
    this._className = className;
    this._viewName = viewName;
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
}
