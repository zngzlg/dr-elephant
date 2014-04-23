package model;

import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.util.Utils;
import play.db.ebean.Model;

import javax.persistence.*;

@Entity
public class JobHeuristicResult extends Model {
    @Id
    public int id;

    @ManyToOne(cascade = CascadeType.ALL)
    public JobResult job;

    @Column
    public Severity severity;

    @Column
    public String analysisName;

    @Lob
    public String data;

    @Column
    public int dataColumns;

    public String[][] getDataArray() {
        return Utils.parseCsvLines(data);
    }
}
