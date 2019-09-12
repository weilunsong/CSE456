import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class test {
    public static void main(String[] args) throws Exception {
        String[] strs = {"1","3","12","33"};
        String ids="1,2,33,45,67";
        List<String> sList = Arrays.asList(strs);
        List<String> idsStringList = Arrays.asList(ids.split(","));
        List<Integer> idsList = new ArrayList<>();

        //            int counterIndex = 0;
//            HashMap<String, List<Integer>> leftCol = tableLeft.getIndex(colLeft.getColumnName());
//            int counter = 1;
//            while (rightIterator.hasNext()){
//                if (counterIndex < indexInright.size() && counter == indexInright.get(counterIndex)) {
//                    Tuple t = getTuple(rightIterator, tableRight);
//                    PrimitiveValue p = t.getAttributes().get(colRight.getColumnName());
//                    if (leftCol.get(p.toRawString()) != null) {
//                        for (Integer indexInfile : leftCol.get(p.toRawString())) {
//                            if (tableLeft.getFile2Current().get(indexInfile) != null) {
//                                int index = tableLeft.getFile2Current().get(indexInfile);
//                                queryResult.add(t.joinTuple(tableLeft.getTupleList().get(index)));
//
//                            }
//                        }
//                    }
//                    counterIndex++;
//                    counter++;
//                } else if (counterIndex == indexInright.size()) {
//                    break;
//                } else {
//                    rightIterator.next();
//                    counter++;
//                }
//            }
    }}
