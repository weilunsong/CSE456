package edu.buffalo.www.cse4562.RA;


import net.sf.jsqlparser.expression.Expression;

public class RASelection extends RANode {

    private String operation = "SELECTION";
    private Expression expression;

    public RASelection(Expression where) {
        this.expression = where;
    }

    public String getOperation() {
        return operation;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression where) {
        this.expression = where;
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
