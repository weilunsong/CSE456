package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import edu.buffalo.www.cse4562.Table.TupleComparator;

import java.util.List;

public class RAOrderby extends RANode {
    private String operation = "ORDERBY";
    private List orderby;

    public RAOrderby(List orderby) {
        this.orderby = orderby;
    }

    public List getOrderby() {
        return orderby;
    }

    public void setOrderby(List orderby) {
        this.orderby = orderby;
    }

    public String getOperation() {
        return operation;
    }

    public TableObject Eval(TableObject table){
        List<Tuple> tupleList = table.getTupleList();
        tupleList.sort(new TupleComparator(orderby));
        table.settupleList(tupleList);
        return table;
    }
    public static TableObject Eval(TableObject table,List orderby){
        List<Tuple> tupleList = table.getTupleList();
        tupleList.sort(new TupleComparator(orderby));
        table.settupleList(tupleList);
        return table;
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
