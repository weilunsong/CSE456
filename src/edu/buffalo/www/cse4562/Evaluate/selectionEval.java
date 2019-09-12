package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.RA.RAJoin;
import edu.buffalo.www.cse4562.RA.RANode;
import edu.buffalo.www.cse4562.RA.RATable;
import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;

import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class selectionEval {
    static Logger logger = Logger.getLogger(selectionEval.class.getName());

    private List<Expression> expressions;
    private Expression where;

    // selection pushdown
    // divide expression in selection into several subexpressions
    // check if the subexpression is related with the table
    // if yes assign the subexpression to the join
    public selectionEval() {

    }

    //used for projection pushdown
    public selectionEval(Expression expression) {

        where = expression;
        this.expressions = new ArrayList<>();
        //if the where does not contain OR, pushdown
        if (expression instanceof AndExpression) {
            Expression left = ((AndExpression) expression).getLeftExpression();
            Expression right = ((AndExpression) expression).getRightExpression();
            //the left and right subExpression of OrExpression should be with the same table.Should not be divided
            if (!(right instanceof AndExpression)) {
                expressions.add(right);
            } else {
                expressions.addAll(parseAndOrExpression(right));
            }
            if (!(left instanceof AndExpression)) {
                expressions.add(left);
            } else {
                expressions.addAll(parseAndOrExpression(left));
            }
        } else {
            expressions.add(expression);
        }
    }

    //used for selection pushdown
    public selectionEval(Expression expression, boolean a) {

        where = expression;
        this.expressions = new ArrayList<>();
        //if the where does not contain OR, pushdown
        if (expression instanceof AndExpression) {
            Expression left = ((AndExpression) expression).getLeftExpression();
            Expression right = ((AndExpression) expression).getRightExpression();
            //the left and right subExpression of OrExpression should be with the same table.Should not be divided
            if (!(right instanceof AndExpression)) {
                expressions.add(right);
            } else {
                expressions.addAll(parseAndExpression(right));
            }
            if (!(left instanceof AndExpression)) {
                expressions.add(left);
            } else {
                expressions.addAll(parseAndExpression(left));
            }
        } else {
            expressions.add(expression);
        }
    }

    public List<Expression> parseAndOrExpression(Expression expression) {
        List<Expression> list = new ArrayList<>();
        if (expression instanceof AndExpression) {
            Expression left = ((AndExpression) expression).getLeftExpression();
            Expression right = ((AndExpression) expression).getRightExpression();
            if (left instanceof AndExpression || left instanceof OrExpression) {
                list.addAll(parseAndOrExpression(left));
            } else {
                list.add(left);
            }
            if (right instanceof AndExpression || right instanceof OrExpression) {
                list.addAll(parseAndOrExpression(right));
            } else {
                list.add(right);
            }
        } else if (expression instanceof OrExpression) {
            Expression left = ((OrExpression) expression).getLeftExpression();
            Expression right = ((OrExpression) expression).getRightExpression();
            if (left instanceof OrExpression || left instanceof AndExpression) {
                list.addAll(parseAndOrExpression(left));
            } else {
                list.add(left);
            }
            if (right instanceof OrExpression || right instanceof AndExpression) {
                list.addAll(parseAndOrExpression(right));
            } else {
                list.add(right);
            }
        }
        return list;
    }

    private List<Expression> parseAndExpression(Expression expression) {
        List<Expression> list = new ArrayList<>();
        if (expression instanceof AndExpression) {
            Expression left = ((AndExpression) expression).getLeftExpression();
            Expression right = ((AndExpression) expression).getRightExpression();
            if (left instanceof AndExpression) {
                list.addAll(parseAndExpression(left));
            } else {
                list.add(left);
            }
            if (right instanceof AndExpression) {
                list.addAll(parseAndExpression(right));
            } else {
                list.add(right);
            }
        }
        return list;
    }

    private int isRelated(Table t, Expression e, List<TableObject> involvedTables) {
        int flag = 0;
//        if (e instanceof BinaryExpression) {
//            flag = judge(t, ((BinaryExpression) e).getLeftExpression(), ((BinaryExpression) e).getRightExpression(), involvedTables);
//        }
        if (e instanceof AndExpression) {
            flag = judge(t, ((AndExpression) e).getLeftExpression(), ((AndExpression) e).getRightExpression(), involvedTables);
        }else if (e instanceof OrExpression){
            Expression eleft = ((OrExpression) e).getLeftExpression();
            Expression eright = ((OrExpression) e).getRightExpression();
            int flagLeft = isRelated(t,eleft,involvedTables);
            int flagRight = isRelated(t,eright,involvedTables);
            if (flagLeft==flagRight&&flagLeft==2){
                flag = 2;
            }
        }else if (e instanceof BinaryExpression){
            flag = judge(t, ((BinaryExpression) e).getLeftExpression(), ((BinaryExpression) e).getRightExpression(), involvedTables);
        }
        return flag;
    }

    private int judge(Table t, Expression le, Expression re, List<TableObject> involvedTables) {
        //flag==1: le re are columns,the expression is related with t
        //flag==2: le or re is column,the expression is related with t only.
        int flag = 0;
        String alisa = t.getAlias();
        if (le instanceof Column && re instanceof Column) {
            Table left = ((Column) le).getTable();
            Table right = ((Column) re).getTable();
            if (left.getName() != null && left.getName().equals(t.getName())
                    || (alisa != null && left.getName().equals(alisa))) {
                flag = 1;
            } else if (right.getName() != null && right.getName().equals(t.getName())
                    || (alisa != null && right.getName().equals(alisa))) {
                flag = 1;
            } else {
                for (TableObject tbo : involvedTables) {
                    if (tbo.getTableName().equals(t.getName()) || (alisa != null && tbo.getTableName().equals(alisa))) {
                        for (int i = 0; i < tbo.getColumnDefinitions().size(); i++) {
                            if (tbo.getColumnDefinitions().get(i).getColumnName().equals(((Column) le).getColumnName())||
                                    tbo.getColumnDefinitions().get(i).getColumnName().equals(((Column) re).getColumnName())) {
                                flag = 1;
                                break;
                            }
                        }
                    }
                }
            }
        } else if (le instanceof Column) {
            Table left = ((Column) le).getTable();
            if (left.getName() != null && left.getName().equals(t.getName())
                    || (alisa != null && left.getName().equals(alisa))) {
                flag = 2;
            } else {
                for (TableObject tbo : involvedTables) {
                    if (tbo.getTableName().equals(t.getName()) || (alisa != null && tbo.getTableName().equals(alisa))) {
                        for (int i = 0; i < tbo.getColumnDefinitions().size(); i++) {
                            if (tbo.getColumnDefinitions().get(i).getColumnName().equals(((Column) le).getColumnName())) {
                                flag = 2;
                                break;
                            }
                        }
                    }
                }
            }
        } else if (re instanceof Column) {
            Table right = ((Column) re).getTable();
            if (right.getName() != null && right.getName().equals(t.getName())
                    || (alisa != null && right.getName().equals(alisa))) {
                flag = 2;
            } else {
                for (TableObject tbo : involvedTables) {
                    if (tbo.getTableName().equals(t.getName()) || (alisa != null && tbo.getTableName().equals(alisa))) {
                        for (int i = 0; i < tbo.getColumnDefinitions().size(); i++) {
                            if (tbo.getColumnDefinitions().get(i).getColumnName().equals(((Column) re).getColumnName())) {
                                flag = 2;
                                ;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return flag;
    }

    private Expression mergeAndExpression(Expression e1, Expression e2) {
        if (e1 != null) {
            return new AndExpression(e1, e2);
        } else {
            return e2;
        }
    }

    private List<Expression> getExpressions() {
        return expressions;
    }

    //parse expression list, get the columns in the expressions
    public List<Column> parseSelect(List<Expression> expressions) {
        List<Column> result = new ArrayList<>();
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = expressions.get(i);
            if (e instanceof BinaryExpression) {
                Expression left = ((BinaryExpression) e).getLeftExpression();
                Expression right = ((BinaryExpression) e).getRightExpression();
                if (left instanceof Column)
                    result.add((Column) left);
                if (right instanceof Column)
                    result.add((Column) right);
            }
        }
        return result;
    }

    //parse where into List including Or
    //todo optimize the idea
    public void parse2List(List<Expression> expressions, Expression expression) {
        if (expression instanceof AndExpression) {
            Expression left = ((AndExpression) expression).getLeftExpression();
            Expression right = ((AndExpression) expression).getRightExpression();
            //the left and right subExpression of OrExpression should be with the same table.Should not be divided
            if (right instanceof AndExpression || right instanceof OrExpression) {
                expressions.addAll(parseAndOrExpression(right));
            } else {
                expressions.add(right);
            }
            if (left instanceof AndExpression || left instanceof OrExpression) {
                expressions.addAll(parseAndOrExpression(left));
            } else {
                expressions.add(left);
            }
        } else if (expression instanceof OrExpression) {
            Expression left = ((OrExpression) expression).getLeftExpression();
            Expression right = ((OrExpression) expression).getRightExpression();
            parse2List(expressions, left);
            parse2List(expressions, right);
        } else {
            expressions.add(expression);
        }
    }

    public int pushdownSelect(RANode pointer, Expression where, List<TableObject> involvedTables) {
        //存在 join 情况
        Expression newWhere = null;
        selectionEval exp = new selectionEval(where, false);
        List<Expression> expList = exp.getExpressions();
        // 0 not related,1 join on condition ,2 table filter
        int flag = 0;
        List<Integer> deleteExp = new ArrayList<>();
        for (int i = 0; i < expList.size(); i++) {
            flag = 0;
            if (pointer.getRightNode() instanceof RATable) {
                flag = exp.isRelated(((RATable) pointer.getRightNode()).getTable(), expList.get(i), involvedTables);
                if (flag == 1)
                    ((RAJoin) pointer).addAndExpression(expList.get(i));
                else if (flag == 2)
                    ((RATable) pointer.getRightNode()).addAndExpression(expList.get(i));

            } else if (pointer.getRightNode() instanceof RAJoin) {
                flag = pushdownSelect(pointer.getRightNode(), expList.get(i), involvedTables);
            }
            //加入！flag 防止同样条件被添加2次
            if (flag == 0 && pointer.getLeftNode() instanceof RATable) {
                flag = exp.isRelated(((RATable) pointer.getLeftNode()).getTable(), expList.get(i), involvedTables);
                if (flag == 1)
                    ((RAJoin) pointer).addAndExpression(expList.get(i));
                else if (flag == 2)
                    ((RATable) pointer.getLeftNode()).addAndExpression(expList.get(i));
            } else if (flag == 0 && pointer.getLeftNode() instanceof RAJoin) {
                flag = pushdownSelect(pointer.getLeftNode(), expList.get(i), involvedTables);
            }
            if (flag != 0) {
                deleteExp.add(i);
            }
        }

        for (int i = deleteExp.size() - 1; i > -1; i--) {//从大往小删
            expList.remove((int) deleteExp.get(i));
        }
        for (Expression e : expList) {
            newWhere = exp.mergeAndExpression(newWhere, e);
        }
        this.where = newWhere;
        return flag;
    }

    public Expression getWhere() {
        return where;
    }
}
