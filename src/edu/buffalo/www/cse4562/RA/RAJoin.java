package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.FromItem;


public class RAJoin extends RANode {
    private String operation = "JOIN";
    private FromItem fromItem;
    private Expression expression;

    public RAJoin(FromItem fromItem) {
        this.fromItem = fromItem;
        this.expression = new EqualsTo(new LongValue(1), new LongValue(1));
    }

    public void addAndExpression(Expression e){
        if (this.expression.toString().equals("1 = 1")){
            this.expression = e;
        }else {
            this.expression = new AndExpression(this.expression,e);
        }

    }
    public void Eval(){

    }

    public FromItem getFromItem() {
        return fromItem;
    }


    public String getOperation() {
        return operation;
    }


    public boolean hasNext() {
        if (this.leftNode != null || this.rightNode != null) {
            return true;
        } else {
            return false;
        }
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public RANode next() {
        return this.leftNode;
    }
}
