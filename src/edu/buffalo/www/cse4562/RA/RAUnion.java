package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.statement.select.Union;

public class RAUnion extends RANode{

    private String operation ="UNION";
    private Union union = new Union();

    public RAUnion(Union union) {
        this.union = union;
    }

    public Union getUnion() {
        return union;
    }

    public void setUnion(Union union) {
        this.union = union;
    }

    public String getOperation() {
        return operation;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Object next() {
        return null;
    }
}
