package edu.buffalo.www.cse4562.Table;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.Comparator;
import java.util.List;

public class TupleComparator implements Comparator<Tuple> {
    private List<OrderByElement> orderByElements;

    public TupleComparator(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }

    @Override
    public int compare(Tuple left, Tuple right) {
        if (orderByElements == null) return 0;

        for (OrderByElement element : orderByElements) {
            Column column = ((Column) element.getExpression());
            PrimitiveValue leftValue = left.getAttributes().get(column.getColumnName());
            PrimitiveValue rightValue = right.getAttributes().get(column.getColumnName());

            try {
                if (leftValue instanceof LongValue) {
                    if (leftValue.toLong() > rightValue.toLong()) {
                        if (element.isAsc())
                            return 1;
                        else
                            return -1;
                    } else if (leftValue.toLong() < rightValue.toLong()) {
                        if (element.isAsc())
                            return -1;
                        else
                            return 1;
                    }
                } else if (leftValue instanceof DoubleValue) {
                    if (leftValue.toDouble() > rightValue.toDouble()) {
                        if (element.isAsc())
                            return 1;
                        else
                            return -1;
                    } else if (leftValue.toDouble() < rightValue.toDouble()) {
                        if (element.isAsc())
                            return -1;
                        else
                            return 1;
                    }
                } else if (leftValue instanceof StringValue) {
                    if (leftValue.toString().compareTo(rightValue.toString()) > 0) {
                        if (element.isAsc())
                            return 1;
                        else
                            return -1;
                    } else if (leftValue.toString().compareTo(rightValue.toString()) < 0) {
                        if (element.isAsc())
                            return -1;
                        else
                            return 1;
                    }
                } else if (leftValue instanceof DateValue) {
                    if (((DateValue) leftValue).getValue().getTime() > ((DateValue) rightValue).getValue().getTime()) {
                        if (element.isAsc())
                            return 1;
                        else
                            return -1;
                    } else if (((DateValue) leftValue).getValue().getTime() < ((DateValue) rightValue).getValue().getTime()) {
                        if (element.isAsc())
                            return -1;
                        else
                            return 1;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 0;
    }
}
