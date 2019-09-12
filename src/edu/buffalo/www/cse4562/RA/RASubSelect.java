package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.schema.Column;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.ArrayList;
import java.util.List;

public class RASubSelect extends RANode {
    private String SubSelectName = null;
    private String Operation = "SUBSELECT";
    private List<ColumnDefinition> usedColDef = new ArrayList<>();
    private List<Column> usedColInf = new ArrayList<>();

    public RASubSelect(String subSelectName) {
        SubSelectName = subSelectName;
    }

    public String getSubSelectName() {
        return SubSelectName;
    }


    public void setSubSelectName(String subSelectName) {
        SubSelectName = subSelectName;
    }

    public void addItemIntoColInf(Column c) {
        this.usedColInf.add(c);
    }


    @Override
    public String getOperation() {
        return this.Operation;
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
    public Object next() {
        return this.leftNode;
    }
}
