package edu.buffalo.www.cse4562.Optimizer;

import edu.buffalo.www.cse4562.Evaluate.selectionEval;
import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class joinOptimizer {
    public static List<Column> Optimizer(FromItem fromItem, List<Join> joins, Expression where, HashMap<String, TableObject> tableMap) {
        List<TableObject> usedTable = new ArrayList<>();
        if (fromItem instanceof SubSelect) {

        } else {
            String tableName = ((Table) fromItem).getName();
            usedTable.add(tableMap.get(tableName));
        }
        for (int i = 0; i < joins.size(); i++) {
            FromItem join = joins.get(i).getRightItem();
            if (join instanceof SubSelect) {

            } else {
                String tableName = ((Table) join).getName();
                usedTable.add(tableMap.get(tableName));
            }
        }
        selectionEval select = new selectionEval(where);
        List<Expression> whereList = select.parseAndOrExpression(where);
        for (Expression e:whereList){
            if (e instanceof EqualsTo){
                if ((((EqualsTo)e).getLeftExpression() instanceof Column&&((EqualsTo)e).getRightExpression() instanceof PrimitiveValue)||
                        (((EqualsTo)e).getRightExpression() instanceof Column&&((EqualsTo)e).getLeftExpression() instanceof PrimitiveValue)){

                }
                    //A = 1

            }
        }
        return null;
    }

}

