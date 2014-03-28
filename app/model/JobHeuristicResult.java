package model;

import com.linkedin.drelephant.analysis.Severity;
import play.db.ebean.Model;

import javax.persistence.*;
import java.util.List;
import java.util.Vector;

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
        if (data.isEmpty()) {
            return new String[0][];
        }
        String[] lines = data.split("\n");
        String[][] result = new String[lines.length][];
        for (int i = 0; i < lines.length; i++) {
            result[i] = parseCsvLine(lines[i]).toArray(new String[0]);
        }
        return result;
    }

    public static List<String> parseCsvLine(String line) {
        Vector<String> store = new Vector<String>();
        StringBuffer curVal = new StringBuffer();
        boolean inquotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (inquotes) {
                if (ch == '\"') {
                    inquotes = false;
                } else {
                    curVal.append(ch);
                }
            } else {
                if (ch == '\"') {
                    inquotes = true;
                    if (curVal.length() > 0) {
                        //if this is the second quote in a value, add a quote
                        //this is for the double quote in the middle of a value
                        curVal.append('\"');
                    }
                } else if (ch == ',') {
                    store.add(curVal.toString());
                    curVal = new StringBuffer();
                } else {
                    curVal.append(ch);
                }
            }
        }
        store.add(curVal.toString());
        return store;
    }
}
