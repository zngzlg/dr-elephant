package com.linkedin.drelephant.spark;

import java.util.List;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.storage.StorageStatus;


/**
 * This class holds information related to Spark storage (RDDs specifically) information.
 *
 */
public class SparkStorageData {
  private List<RDDInfo> _rddInfoList;
  private List<StorageStatus> _storageStatusList;

  public List<RDDInfo> getRddInfoList() {
    return _rddInfoList;
  }

  public void setRddInfoList(List<RDDInfo> rddInfoList) {
    _rddInfoList = rddInfoList;
  }

  public List<StorageStatus> getStorageStatusList() {
    return _storageStatusList;
  }

  public void setStorageStatusList(List<StorageStatus> storageStatusList) {
    _storageStatusList = storageStatusList;
  }
}
