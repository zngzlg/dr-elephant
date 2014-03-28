package model;

import com.linkedin.drelephant.analysis.Severity;
import play.db.ebean.Model;

import javax.persistence.*;
import java.util.List;

@Entity
public class JobResult extends Model {

    @Id
    @Column(length = 50)
    public String job_id;

    @Column(length = 50)
    public String username;

    @Column(length = 100)
    public String jobName;

    @Column
    public long startTime;

    @Column
    public long analysisTime;

    @Column
    public Severity severity;

    @Column(length = 200)
    public String url;

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "job")
    public List<JobHeuristicResult> heuristicResults;

    public static Finder<String, JobResult> find = new Finder<String, JobResult>(
            String.class, JobResult.class
    );
}
