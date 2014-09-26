package com.linkedin.drelephant.util;

import java.util.ArrayList;
import java.util.List;



public class HeuristicConfData {
  private String heuristicName;
  private String className;
  private String viewName;
  private List<String> hadoopVersions;

  public HeuristicConfData(String heuristicName,String className, String viewName, List<String> hadoopVersions)
  {
    this.heuristicName = heuristicName;
    this.className = className;
    this.viewName = viewName;
    this.hadoopVersions = hadoopVersions;
  }
  public String getHeuristicName() {
    return heuristicName;
  }

  public String getClassName() {
    return className;
  }

  public String getViewName() {
    return viewName;
  }

  public List<String> getHadoopVersions() {
    return hadoopVersions;
  }

}
