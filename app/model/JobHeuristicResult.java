package model;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.util.Utils;

import play.db.ebean.Model;


@Entity
public class JobHeuristicResult extends Model {

  private static final long serialVersionUID = 123L;

  @JsonIgnore
  @Id
  public int id;

  @JsonBackReference
  @ManyToOne(cascade = CascadeType.ALL)
  public JobResult job;

  @Column
  public Severity severity;

  @Column
  public String analysisName;

  @JsonIgnore
  @Lob
  public String data;

  @JsonIgnore
  @Column
  public int dataColumns;

  public String[][] getDataArray() {
    return Utils.parseCsvLines(data);
  }
}
