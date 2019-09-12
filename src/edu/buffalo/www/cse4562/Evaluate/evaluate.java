package edu.buffalo.www.cse4562.Evaluate;

//import edu.buffalo.www.cse4562.Table.Column;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

public class evaluate extends Eval {
    static Logger logger = Logger.getLogger(evaluate.class.getName());
    private Tuple tupleLeft;
    private Tuple tupleRight;
    private Expression expression;
    private List<Object> selectList;

    public evaluate(Tuple tupleLeft, Tuple tupleRight, Expression expression) {
        //selection
        this.tupleLeft = tupleLeft;
        this.tupleRight = tupleRight;
        this.expression = expression;
    }

    public evaluate(Tuple tupleLeft) {
        //projection
        this.tupleLeft = tupleLeft;
    }

    public evaluate(List<Object> list){
        this.selectList = list;
    }
    @Override
    public PrimitiveValue eval(net.sf.jsqlparser.schema.Column column) throws SQLException {
        String colTable = column.getTable().getName();
        if (colTable != null) {
            if (tupleLeft.getTableName().contains(colTable) || tupleRight == null) {
                return tupleLeft.getAttributes().get(column.getColumnName());
            } else {
                return tupleRight.getAttributes().get(column.getColumnName());
            }
        } else {
            if (tupleLeft.getAttributes().containsKey(column.getColumnName()) || tupleRight == null) {
                return tupleLeft.getAttributes().get(column.getColumnName());
            } else {
                return tupleRight.getAttributes().get(column.getColumnName());
            }
        }
    }

    public List<Tuple> Eval(List<Tuple> queryResult) throws Exception {
        //Select and Join expression evaluate.
        PrimitiveValue result = eval(this.expression);
        try {
            if (result.toBool()) {
                Tuple tuple = this.tupleRight != null ? this.tupleLeft.joinTuple(this.tupleRight) : this.tupleLeft;
                queryResult.add(tuple);
            }
        }catch (Exception e){
            e.printStackTrace();
            logger.info(this.expression.toString());
            logger.info(tupleLeft.getAttributes().toString());
            if (result==null){
                logger.info("result is null");
            }
            logger.info(tupleRight.getAttributes().toString());
        }
        return queryResult;
    }


//    public Tuple projectEval(List<Column> columns, Table table) throws Exception {
//        //todo 查表后进行 projection，优化，利用新列定义，不解析 selectItem
//        Tuple newTuple = new Tuple();
//        HashMap<Column, PrimitiveValue> attributes = new HashMap<>();
//        for (int i = 0; i < selectList.size(); i++) {
//            Object s = selectList.get(i);
//            if (s instanceof AllColumns) {
//                //todo
//                newTuple = tupleLeft;
//                break;
//            } else if (s instanceof AllTableColumns) {
//                String tableName = ((AllTableColumns) s).getTable().getName();
//                for (int j = 0; j < columns.size(); j++) {
//                    if (columns.get(j).getTable().getName().equals(tableName)) {
//                        Column column = new Column(columns.get(j).getTable(), columns.get(j).getColumnName());
////                        Column column = columns.get(j);
//                        attributes.put(column, tupleLeft.getAttributes().get(columns.get(j)));
//                        if (!table.toString().equals("null")) {
//                            column.setTable(table);
//                        }
//                    }
//                }
//            } else {
//                //  todo 优化
//                if (((SelectExpressionItem) s).getExpression() instanceof Column) {
//                    String alias = ((SelectExpressionItem) s).getAlias();
//                    Column column = new Column(((Column) ((SelectExpressionItem) s).getExpression()).getTable(), ((Column) ((SelectExpressionItem) s).getExpression()).getColumnName());
//                    if (alias == null) {
//                        attributes.put(column, tupleLeft.getAttributes().get(((SelectExpressionItem) s).getExpression()));
//                    } else {
//                        column.setColumnName(alias);
//                        attributes.put(column, tupleLeft.getAttributes().get(((SelectExpressionItem) s).getExpression()));
//                    }
//                    if (!table.toString().equals("null")) {
//                        column.setTable(table);
//                    }
//                } else {
//                    PrimitiveValue result = eval(((SelectExpressionItem) s).getExpression());
//                    String name = ((SelectExpressionItem) s).getAlias();
//                    attributes.put(new Column(table, name), result);
//                }
//            }
//        }
//        newTuple.setAttributes(attributes);
//        newTuple.setTableName(tupleLeft.getTableName());
//        return newTuple;
//    }

//    public List project(List<Tuple> tupleList,List<Expression> columnList,List<Column> columnInfo)throws Exception{
//        List<Tuple> newTupleList = new ArrayList<>();
//        for (int i = 0;i<tupleList.size();i++){
//            Tuple newTuple = new Tuple();
//            HashMap<Column, PrimitiveValue> attributes = new HashMap<>();
//            for (int j = 0;j<columnList.size();j++){
//                if (columnList.get(j)!=null){
//                    attributes.put(columnInfo.get(j),tupleList.get(i).getAttributes().get(columnList.get(j)));
//                }else {
//                    Expression e= ((SelectExpressionItem) selectList.get(j)).getExpression();
//                    this.tupleLeft = tupleList.get(i);
//                    attributes.put(columnInfo.get(j),eval(e));
//                }
//            }
//            newTuple.setAttributes(attributes);
//            newTupleList.add(newTuple);
//        }
//        return newTupleList;
//    }

}



