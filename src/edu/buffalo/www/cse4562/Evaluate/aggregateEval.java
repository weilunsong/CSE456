package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.*;

import java.util.*;
import java.util.logging.Logger;

public class aggregateEval {
    static Logger logger = Logger.getLogger(aggregateEval.class.getName());

    private List<Tuple> tupleList = new ArrayList<>();
    private List<Function> functionList = new ArrayList<>();


    public aggregateEval(List<Tuple> tupleList, List<Function> functionList) {
        this.tupleList = tupleList;
        this.functionList = functionList;
    }

    public List<PrimitiveValue> eval() {
        List<PrimitiveValue> result = new ArrayList<>();
        for (int i = 0; i < functionList.size(); i++) {
            //initiate
            result.add(null);
        }
        boolean hasAVG = false;
        try {
            for (Tuple tuple : tupleList) {
                for (int i = 0; i < functionList.size(); i++) {
                    Function f = functionList.get(i);
                    if (f.getName().toUpperCase().equals("COUNT")) {
                        result.set(i, new LongValue(tupleList.size()));
                    } else if (f.getName().toUpperCase().equals("MIN")) {
                        result.set(i, min(f, tuple, result.get(i)));
                    } else if (f.getName().toUpperCase().equals("MAX")) {
                        result.set(i, max(f, tuple, result.get(i)));
                    } else if (f.getName().toUpperCase().equals("SUM")) {
                        result.set(i, sum(f, tuple, result.get(i)));
                    } else if (f.getName().toUpperCase().equals("AVG")) {
                        hasAVG = true;
                        result.set(i, sum(f, tuple, result.get(i)));
                    }
                }
            }
            if (hasAVG) {
                for (int i = 0; i < functionList.size(); i++) {
                    Function f = functionList.get(i);
                    if (f.getName().toUpperCase().equals("AVG")) {
                        result.set(i, avg(result.get(i)));
                    }
                }
            }
        } catch (Exception e) {
            logger.warning("aggregate function ");
            e.printStackTrace();
        }


        return result;
    }

    private PrimitiveValue sum(Function f, Tuple t, PrimitiveValue value) throws Exception {
        evaluate eval = new evaluate(t);
        Expression exp = f.getParameters().getExpressions().get(0);
        PrimitiveValue val = eval.eval(exp);
        if (value != null) {
            if (val instanceof LongValue) {
                value = new LongValue(value.toLong() + val.toLong());
            } else if (val instanceof DoubleValue) {
                value = new DoubleValue(value.toDouble() + val.toDouble());
            }
        } else {
            if (val instanceof LongValue) {
                value = new LongValue(val.toLong());
            } else if (val instanceof DoubleValue) {
                value = new DoubleValue(val.toDouble());
            }
        }
//        BigDecimal bg = new BigDecimal(value.toDouble());
//        double f1 = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
//        return new DoubleValue(f1);

        return value;
    }


    private PrimitiveValue avg(PrimitiveValue value) throws Exception {
        DoubleValue avg = null;
        if (value instanceof DoubleValue) {
            avg = new DoubleValue(value.toDouble() / tupleList.size());
        } else if (value instanceof LongValue) {
            avg = new DoubleValue(value.toLong() / tupleList.size());
        }
        return avg;
    }

    private PrimitiveValue min(Function f, Tuple t, PrimitiveValue value) throws Exception {
        evaluate eval = new evaluate(t);
        Expression exp = f.getParameters().getExpressions().get(0);
        PrimitiveValue val = eval.eval(exp);
        if (value != null) {
            if (val instanceof LongValue) {
                if (val.toLong() < value.toLong()) value = val;
            } else if (val instanceof DoubleValue) {
                if (val.toDouble() < value.toDouble()) value = val;
            } else if (val instanceof StringValue) {
                if (val.toString().compareTo(value.toString()) < 0) value = val;
            } else {
                DateValue newDate = (DateValue) val;
                DateValue originalDate = (DateValue) value;
                if (newDate.getValue().getTime() < originalDate.getValue().getTime()) value = val;
            }
        } else {
            value = val;
        }

        return value;
    }

    private PrimitiveValue max(Function f, Tuple t, PrimitiveValue value) throws Exception {
        evaluate eval = new evaluate(t);
        Expression exp = f.getParameters().getExpressions().get(0);
        PrimitiveValue val = eval.eval(exp);
        if (value != null) {

            if (val instanceof LongValue) {
                if (val.toLong() > value.toLong()) value = val;
            } else if (val instanceof DoubleValue) {
                if (val.toDouble() > value.toDouble()) value = val;
            } else if (val instanceof StringValue) {
                if (val.toString().compareTo(value.toString()) > 0) value = val;
            } else {
                DateValue newDate = (DateValue) val;
                DateValue originalDate = (DateValue) value;
                if (newDate.getValue().getTime() > originalDate.getValue().getTime()) value = val;
            }
        } else {
            value = val;
        }
        return value;
    }
}
