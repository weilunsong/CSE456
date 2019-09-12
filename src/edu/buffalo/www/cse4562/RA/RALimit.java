package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.statement.select.Limit;

public class RALimit extends RANode {

    private String operation = "LIMIT";
    private Limit limit;

    public RALimit(Limit limit) {
        this.limit = limit;
    }

    public Limit getLimit() {
        return limit;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public TableObject Eval(TableObject unLimit){
        if (unLimit.getTupleList().size()>this.limit.getRowCount()){
            unLimit.settupleList(unLimit.getTupleList().subList(0,(int) this.limit.getRowCount()));
        }
        return unLimit;
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
