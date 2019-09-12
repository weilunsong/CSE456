package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Evaluate.aggregateEval;
import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

public class RAProjection extends RANode {

    private String operation = "PROJECTION";
    private List<SelectItem> selectItem;
    private boolean flag = false;//record select *
    private List<Expression> columnList = new ArrayList<>();
    private List<Column> columnInfo = new ArrayList<>();
    private boolean isGroupBy = false;

    public RAProjection(List<SelectItem> selectItem) {
        this.selectItem = selectItem;
        if (selectItem != null && selectItem.get(0) instanceof AllColumns) {
            this.flag = true;
        }
    }

    public List getSelectItem() {
        return selectItem;
    }

    public TableObject Eval(TableObject OutputTable, String tableName) throws Exception {
        //table name :the sub query's alisa
        if (!flag) {
            if (isGroupBy) {
                projectionParser(OutputTable.getColumnInfo(), new Table(tableName));
                Iterator<Map.Entry<Integer, ArrayList<Tuple>>> iterator = OutputTable.getgroupMap().entrySet().iterator();
                ArrayList<Function> functions = extractFunc();
                while (iterator.hasNext()) {
                    //process aggregate
                    List<Tuple> tupleList = iterator.next().getValue();
                    aggregateEval aggEval = new aggregateEval(tupleList, functions);
                    List funcVal = aggEval.eval();
                    OutputTable.addTupleList(project(tupleList.subList(0,1), this.columnList, this.columnInfo, funcVal));
                }
            } else {
                projectionParser(OutputTable.getColumnInfo(), new Table(tableName));
                //evaluate eva = new evaluate(this.selectItem);
                //OutputTable.settupleList(eva.project(OutputTable.getTupleList(),this.columnList,this.columnInfo));
                OutputTable.settupleList(project(OutputTable.getTupleList(), this.columnList, this.columnInfo, null));

            }
        } else {
            if (tableName != null) {
                for (int i = 0; i < OutputTable.getColumnInfo().size(); i++) {
                    OutputTable.getColumnInfo().get(i).setTable(new Table(tableName));
                }
            }
        }
        return OutputTable;
    }

    public void setSelectItem(List selectItem) {
        this.selectItem = selectItem;
    }

    public boolean isGroupBy() {
        return isGroupBy;
    }

    public void setisGroupBy(boolean isGroupBy) {
        this.isGroupBy = isGroupBy;
    }

    public String getOperation() {
        return this.operation;
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

    public void projectionParser(List<Column> columns, Table table) throws Exception {
        //columns：the columnInfo of result
        // 查表后进行 projection，优化，利用新列定义，不解析 selectItem
        for (int i = 0; i < selectItem.size(); i++) {
            Object s = selectItem.get(i);
            if (s instanceof AllTableColumns) {
                //select R.*
                String tableName = ((AllTableColumns) s).getTable().getName();
                for (int j = 0; j < columns.size(); j++) {
                    if (columns.get(j).getTable().getName().equals(tableName)) {
                        Column column = new Column(columns.get(j).getTable(), columns.get(j).getColumnName());
                        columnList.add(columns.get(j));
                        columnInfo.add(column);
                        if (!table.toString().equals("null")) {
                            column.setTable(table);
                        }
                    }
                }
            } else if (((SelectExpressionItem) s).getExpression() instanceof Column) {
                //SELECT *
                String alias = ((SelectExpressionItem) s).getAlias();
                Column column = new Column(((Column) ((SelectExpressionItem) s).getExpression()).getTable(), ((Column) ((SelectExpressionItem) s).getExpression()).getColumnName());
                if (alias == null) {
                    columnList.add(((SelectExpressionItem) s).getExpression());
                    columnInfo.add(column);
                } else {
                    column.setColumnName(alias);
                    columnList.add(((SelectExpressionItem) s).getExpression());
                    columnInfo.add(column);
                }
                if (!table.toString().equals("null")) {
                    column.setTable(table);
                }
            } else if (((SelectExpressionItem) s).getExpression() instanceof Function) {
                if (((SelectExpressionItem) s).getAlias() != null) {
                    columnInfo.add(new Column(table, ((SelectExpressionItem) s).getAlias()));
                } else {
                    columnInfo.add(new Column(table, s.toString()));
                }
                columnList.add(((SelectExpressionItem) s).getExpression());
            } else {
                String name = ((SelectExpressionItem) s).getAlias();
                columnList.add(null);
                columnInfo.add(new Column(table, name));
            }
        }
    }

    public List project(List<Tuple> tupleList, List<Expression> columnList, List<Column> columnInfo, List<PrimitiveValue> funcVals) throws Exception {
        List<Tuple> newTupleList = new ArrayList<>();
//        if (funcVals==null){
//            for (int i = 0; i < tupleList.size(); i++) {
//                Tuple newTuple = new Tuple();
//                HashMap<Column, PrimitiveValue> attributes = new HashMap<>();
//                for (int j = 0; j < columnList.size(); j++) {
//                    if (columnList.get(j) != null) {
//                        attributes.put(columnInfo.get(j), tupleList.get(i).getAttributes().get(columnList.get(j)));
//                    } else {
//                        Expression e = ((SelectExpressionItem) selectItem.get(j)).getExpression();
//                        evaluate eval = new evaluate(tupleList.get(i));
//                        attributes.put(columnInfo.get(j), eval.eval(e));
//                    }
//                }
//                newTuple.setAttributes(attributes);
//                newTupleList.add(newTuple);
//            }
//        }else {
        for (int i = 0; i < tupleList.size(); i++) {
            Tuple newTuple = new Tuple();
            HashMap<String, PrimitiveValue> attributes = new HashMap<>();
            int index = 0;
            for (int j = 0; j < columnList.size(); j++) {
                if (columnList.get(j) == null) {
                    Expression e = ((SelectExpressionItem) selectItem.get(j)).getExpression();
                    evaluate eval = new evaluate(tupleList.get(i));
                    attributes.put(columnInfo.get(j).getColumnName(), eval.eval(e));
                } else if (columnList.get(j) instanceof Column) {
                    attributes.put(columnInfo.get(j).getColumnName(), tupleList.get(i).getAttributes().get(((Column) (columnList.get(j))).getColumnName()));
                } else if (columnList.get(j) instanceof Function) {
                    if (funcVals!=null){
                        //有groupby，执行sum等操作时，值已经事先求好
                        attributes.put(columnInfo.get(j).getColumnName(), funcVals.get(index++));
                    }else {
                        //当无groupby 但有sum等操作时
                        List<Function> functions = new ArrayList<>();
                        functions.add((Function) columnList.get(j));
                        aggregateEval aggEval = new aggregateEval(tupleList, functions);
                        funcVals = aggEval.eval();
                        attributes.put(columnInfo.get(j).getColumnName(), funcVals.get(0));
                        //todo 强制退出外圈循环，临时办法，待修改
                        tupleList.clear();
                    }
                }
            }
            //newTuple.setAttributes((HashMap<edu.buffalo.www.cse4562.Table.Column,PrimitiveValue>)(HashMap)attributes);
            newTuple.setAttributes(attributes);
            newTupleList.add(newTuple);
        }
        //}

        return newTupleList;
    }

    public ArrayList<Function> extractFunc() {
        ArrayList<Function> functionList = new ArrayList<>();
        for (SelectItem s : selectItem) {
            Expression exp = ((SelectExpressionItem) s).getExpression();
            if (exp instanceof Function) {
                functionList.add((Function) exp);
            }
        }
        return functionList;
    }
}
