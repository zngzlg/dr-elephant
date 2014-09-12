package model;

import javax.persistence.Entity;

import com.avaje.ebean.annotation.Sql;


@Entity
@Sql
public class StringResult {
  String string;

  public void setString(String val) {
    string = val;
  }

  public String getString() {
    return string;
  }
}
