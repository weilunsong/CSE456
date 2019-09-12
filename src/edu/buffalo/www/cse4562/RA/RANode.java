package edu.buffalo.www.cse4562.RA;


import net.sf.jsqlparser.expression.Expression;

import java.util.Iterator;

public abstract class RANode implements Iterator{
    public RANode leftNode;
    public RANode rightNode;
    private String operation;
    private RANode parentNode;
    private Expression expression;

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression e) {
        this.expression = e;
    }

    public RANode getParentNode() {
        return parentNode;
    }

    public void setParentNode(RANode parentNode) {
        this.parentNode = parentNode;
    }

    public RANode getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(RANode leftNode) {
        this.leftNode = leftNode;
    }

    public RANode getRightNode() {
        return rightNode;
    }

    public void setRightNode(RANode rightNode) {
        this.rightNode = rightNode;
    }
    public abstract String getOperation();

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract Object next();
}