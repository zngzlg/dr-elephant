package model;

import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.util.Utils;
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

    @Column
    public JobType jobType;

    @Column(length = 200)
    public String url;

    @Column(length = 100)
    public String cluster;

    @Lob
    public String metaUrls;

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "job")
    public List<JobHeuristicResult> heuristicResults;

    public String[][] getMetaUrls() {
        return Utils.parseCsvLines(metaUrls);
    }

    public static Finder<String, JobResult> find = new Finder<String, JobResult>(
            String.class, JobResult.class
    );
}
