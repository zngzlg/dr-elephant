package model;

import com.avaje.ebean.annotation.EnumValue;


public enum JobType {
  @EnumValue("Hadoop")
  HADOOPJAVA("HadoopJava"),

  @EnumValue("Pig")
  PIG("Pig"),

  @EnumValue("Hive")
  HIVE("Hive");

  private String _text;

  private JobType(String text) {
    this._text = text;
  }

  public String getText() {
    return _text;
  }
}
