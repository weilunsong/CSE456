package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.statement.select.Distinct;

import java.util.List;

public class RADistinct extends RANode {
    private String operation = "DISTINCT";
    private Distinct distinct;

    public RADistinct(Distinct distinct) {
        this.distinct = distinct;
    }

    public List<Tuple> Eval(List<Tuple> list) {
        return list;
    }

    public Distinct getDistinct() {
        return distinct;
    }

    public void setDistinct(Distinct distinct) {
        this.distinct = distinct;
    }

    public String getOperation() {
        return operation;
    }
    @Override
    public boolean hasNext() {
        if (this.leftNode != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public RANode next() {
        return this.leftNode;
    }
}
