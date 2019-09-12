package edu.buffalo.www.cse4562.Table;

import edu.buffalo.www.cse4562.RA.RANode;
import edu.buffalo.www.cse4562.RA.RATable;
import net.sf.jsqlparser.schema.Column;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TableObject {
    private Table table;
    private String tableName;
    private String alisa;
    private String fileDir;
    private boolean original = true;
    //private List<Integer> currentTuple = null;
//    private HashMap<Integer, Integer> file2Current = new HashMap<>();
    private List<ColumnDefinition> columnDefinitions;// record the column type String,Long,Double...
    //when the table is a query result, it is necessary to record the table info about the column
    private List<Column> columnInfo = new ArrayList<>();//record the columns and their table information.
    private List<String> IndexFileName = new ArrayList<>();
    //when the table is a query result, it is necessary to record the table info about the column
    private List<Integer> index = new ArrayList<>();
    private boolean isLeft = true;
    private List<Tuple> tupleList;
    private HashMap<Integer, ArrayList<Tuple>> groupMap = new HashMap<>();
    private HashMap<Integer, ArrayList<Integer>> joinHash = null;
    private List<Integer> mapRelations = new ArrayList<>();
    private List<Column> primaryKey = new ArrayList<>();
    private List<Column> references = new ArrayList<>();

    static Logger logger = Logger.getLogger(TableObject.class.getName());


    public TableObject(TableObject tableObject, RANode raTable) {

        this.table = ((RATable) raTable).getTable();
        this.tableName = ((RATable) raTable).getTable().getName();
        this.alisa = this.table.getAlias();
        this.columnDefinitions = tableObject.getColumnDefinitions();
        this.columnInfo = tableObject.getColumnInfo();
        this.fileDir = tableObject.getFileDir();
//        this.index = tableObject.getIndex();
//        this.statistics = tableObject.getStatistics();
    }

    public TableObject(TableObject tableObject, RANode raTable, String alisa) {
        this.table = ((RATable) raTable).getTable();
        this.tableName = ((RATable) raTable).getTable().getName();
        this.alisa = alisa;
        this.columnDefinitions = tableObject.getColumnDefinitions();
        this.columnInfo = tableObject.getColumnInfo();
    }

    public TableObject() {

    }

    public TableObject(CreateTable createTable, Table table, String tableName) {
        this.table = table;
        this.tableName = tableName;
        this.columnDefinitions = createTable.getColumnDefinitions();
        for (ColumnDefinition c : this.columnDefinitions) {
            Column col = new Column(table, c.getColumnName());
            columnInfo.add(col);
            if (c.getColumnSpecStrings() != null && c.getColumnSpecStrings().contains("PRIMARY")) {
                this.primaryKey.add(col);
            }
            if (c.getColumnSpecStrings() != null && c.getColumnSpecStrings().contains("REFERENCES")) {
                this.references.add(col);
            }
        }
        if (this.fileDir == null) {
            fileDir = "data/" + createTable.getTable().getName() + ".dat";
        }
    }


    public String getAlisa() {
        return alisa;
    }

    public void setAlisa(String alisa) {
        this.alisa = alisa;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFileDir() {
        return fileDir;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    public boolean isOriginal() {
        return original;
    }

    public void setOriginal(boolean original) {
        this.original = original;
    }

    public List<Column> getColumnInfo() {
        return columnInfo;
    }

    public void setColumnInfo(List<Column> columnInfo) {
        this.columnInfo = columnInfo;
    }

//    public List<Integer> getCurrentTuple() {
//        return currentTuple;
//    }
//
//    public void setCurrentTuple(List<Integer> currentTuple) {
//        this.currentTuple = currentTuple;
//    }

//    public HashMap<Integer, Integer> getFile2Current() {
//        return file2Current;
//    }
//
//    public void setFile2Current(HashMap<Integer, Integer> file2Current) {
//        this.file2Current = file2Current;
//    }

    public void MapRelation(List<Column> usedColInfo) {
        //get the map relations between columninfo and usedColumnInfo
        for (int i = 0; i < usedColInfo.size(); i++)
            usedColInfo.get(i).setTable(this.table);
        List<ColumnDefinition> newColDef = new ArrayList<>();
        List<Column> newColInfo = new ArrayList<>();
        for (int i = 0; i < columnInfo.size(); i++) {
            if (usedColInfo.contains(columnInfo.get(i))) {
                newColDef.add(columnDefinitions.get(i));
                newColInfo.add(columnInfo.get(i));
                mapRelations.add(i);
            }
        }
        this.columnInfo = newColInfo;
        this.columnDefinitions = newColDef;
    }

    public List<Integer> getMapRelations() {
        return mapRelations;
    }

    public List<Tuple> getTupleList() {
        return tupleList;
    }


    public void settupleList(List<Tuple> tupleList) {
        this.tupleList = tupleList;
    }

    public void addTupleList(List<Tuple> tupleList) {
        this.tupleList.addAll(tupleList);
    }

    public Iterator getIterator() {
        return this.tupleList.iterator();
    }

    public HashMap<Integer, ArrayList<Tuple>> getgroupMap() {
        return this.groupMap;
    }

//    public void setHashMap(HashMap<Integer, ArrayList<Tuple>> hashMap) {
//        this.groupMap = hashMap;
//    }
//
//    public HashMap<Integer, ArrayList<Integer>> getJoinHash() {
//        return joinHash;
//    }
//
//    public void setJoinHash(HashMap<Integer, ArrayList<Integer>> joinHash) {
//        this.joinHash = joinHash;
//    }
//
//    public void setIndexCSV() throws Exception {
//        HashMap<String, HashMap<String, String>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
//        List<Integer> attrIndex = new ArrayList<>();
//        if (tableName.equals("LINEITEM")) {
//            index.put("L_QUANTITY", new HashMap<>());
//            index.put("L_DISCOUNT", new HashMap<>());
//        }
//        if (tableName.equals("PART")){
//            index.put("P_SIZE", new HashMap<>());
//        }
//
//        for (int i = 0; i < primaryKey.size(); i++) {
//            index.put(primaryKey.get(i).getColumnName(), new HashMap<>());
//        }
//        for (int i = 0; i < references.size(); i++) {
//            index.put(references.get(i).getColumnName(), new HashMap<>());
//        }
//        for (int i = 0; i < columnInfo.size(); i++) {
//            if (index.containsKey(columnInfo.get(i).getColumnName()))
//                attrIndex.add(i);
//        }
//        int i = 1;
//
//        FileReader fs = new FileReader(fileDir);
//        BufferedReader br = new BufferedReader(fs);
//        String line;
//        if (index.size() != 0) {
//            while((line = br.readLine()) != null){
//                String[] tuple = line.split("\\|");
//                for (int j = 0; j < attrIndex.size(); j++) {
//                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
//                    String colName = columnInfo.get(attrIndex.get(j)).getColumnName();
//                    HashMap<String, String> colMap = index.get(colName);
//                    String attrVal = tuple[attrIndex.get(j)];
//                    String originalList = colMap.put(attrVal,"");
//                    if (originalList==null) {
//                        String list = Integer.toString(i);
//                        colMap.put(attrVal, list);
//                    } else {
//                        String list = originalList + "," + Integer.toString(i);
//                        colMap.put(attrVal, list);
//                    }
//                }
//                i++;
//            }
//        }
//        File fileL = new File(fileDir);
//        logger.info(String.valueOf(fileL.length())+"bytes");
//        final String[] FILE_HEADER = {"Column", "Value", "Index"};
//        final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + ".csv";
//        CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER).withSkipHeaderRecord();
//        try (Writer out = new FileWriter(FILE_NAME);
//             CSVPrinter printer = new CSVPrinter(out, format)) {
//            for (String column : index.keySet()) {
//                for (String value : index.get(column).keySet()) {
//                    List<String> records = new ArrayList<>();
//                    records.add(column);
//                    records.add(value);
//                    records.add(index.get(column).get(value));
//                    printer.printRecord(records);
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
    public List<String> getIndexFileName() {
        return IndexFileName;
    }

    public void setIndexFileName(String indexFile) {
        this.IndexFileName.add(0,indexFile);
    }
//    public void setIndexFileName(List<String> indexFile) {
//        this.IndexFileName.addAll(indexFile);
//    }
//
//    public void getIndexTuple()throws Exception{
//        tupleList = new ArrayList<>();
//        for (String fileName:IndexFileName){
//            FileInputStream inputStream = new FileInputStream(fileName);
//            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
//            List<Tuple> temp = (List<Tuple>)objectInputStream.readObject();
//            tupleList.addAll (temp);
//        }
//    }
//    public HashMap<String, HashMap<String, ArrayList<String>>> getIndex1() {
//        final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + ".csv";
//        final String[] FILE_HEADER = {"Column", "Value", "Index"};
//        CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER).withSkipHeaderRecord();
//        HashMap<String, HashMap<String, ArrayList<String>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
//
//        try (Reader in = new FileReader(FILE_NAME)) {
//            Iterable<CSVRecord> records = format.parse(in);
//            for (CSVRecord record : records) {
//                String col = record.get("Column").toUpperCase();
//                //Column c = new Column(new Table(),col);
//                String p = record.get("Value");
//                String[] indseq = record.get("Index").replace(" ", "").replace("[", "").replace("]", "").split(",");
//                ArrayList<String> list = new ArrayList<>(Arrays.asList(indseq));
//                if (index.containsKey(col)) {
//                    index.get(col).put(p, list);
//                } else {
//                    HashMap<String, ArrayList<String>> hashMap = new HashMap<>();
//                    hashMap.put(p, list);
//                    index.put(col, hashMap);
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return index;
//    }
//
//
//    public void setIndexTXTDivide(int part) throws Exception {
//        //有划分，1列一文件
//        for (int k =0;k<part;k++){
//            HashMap<String, HashMap<String, List<Integer>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
//            List<Integer> attrIndex = new ArrayList<>();
//            for (int i = k*columnInfo.size()/part; i < (k+1)*columnInfo.size()/part; i++) {
//                index.put(columnInfo.get(i).getColumnName(), new HashMap<>());
//            }
//            if (index.size()==0){
//                continue;
//            }
//            for (int i = 0; i < columnInfo.size(); i++) {
//                if (index.containsKey(columnInfo.get(i).getColumnName()))
//                    attrIndex.add(i);
//            }
//            int i = 1;
//            FileReader fs = new FileReader(fileDir);
//            BufferedReader br = new BufferedReader(fs);
//            String line;
//            if (index.size() != 0) {
//                while((line = br.readLine()) != null){
//                    String[] tuple = line.split("\\|");
//                    for (int j = 0; j < attrIndex.size(); j++) {
//                        //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
//                        String colName = columnInfo.get(attrIndex.get(j)).getColumnName();
//                        String attrVal = tuple[attrIndex.get(j)];
//                        if (!index.get(colName).containsKey(attrVal)) {
//                            ArrayList<Integer> a = new ArrayList<>();
//                            a.add(i);
//                            index.get(colName).put(attrVal,a);
//                        } else {
//                            index.get(colName).get(attrVal).add(i);
//                            //index.get(colName).put(attrVal,index.get(colName).get(attrVal).concat(",").concat(String.valueOf(i)));
//                        }
//                    }
//
//                    i++;
//                }
//            }
//            for (String colName :index.keySet()){
//                File file = new File("indexes/" + this.getTableName().toUpperCase()+"_"+colName + ".txt");
//                if (!file.exists()) {
//                    file.createNewFile();
//                }
//                FileOutputStream outputStream = new FileOutputStream(file);
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
//                objectOutputStream.writeObject(index.get(colName));
//                objectOutputStream.close();
//            }
//
//        }
//    }
//
//    public void setIndexTXT() throws Exception {
//        //无划分，一列一文件
//        HashMap<String, HashMap<String, StringBuilder>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
//        for (int i = 0; i < columnInfo.size(); i++) {
//            index.put(columnInfo.get(i).getColumnName(), new HashMap<>());
//        }
//
//        int i = 1;
//        FileReader fs = new FileReader(fileDir);
//        BufferedReader br = new BufferedReader(fs);
//        String line;
//        if (index.size() != 0) {
//            while((line = br.readLine()) != null){
//                String[] tuple = line.split("\\|");
//                for (int j = 0; j < columnInfo.size(); j++) {
//                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
//                    String colName = columnInfo.get(j).getColumnName();
//                    String attrVal = tuple[j];
//                    if (!index.get(colName).containsKey(attrVal)) {
//                        index.get(colName).put(attrVal,new StringBuilder(String.valueOf(i)));
//                    } else {
//                        index.get(colName).put(attrVal, index.get(colName).get(attrVal).append(",").append(i));
//                    }
//                }
//
//                i++;
//            }
//        }
//        for (String colName :index.keySet()){
//            File file = new File("indexes/" + this.getTableName().toUpperCase()+"_"+colName + ".txt");
//            if (!file.exists()) {
//                file.createNewFile();
//            }
//            FileWriter fw = new FileWriter(file, true);
//            BufferedWriter bw = new BufferedWriter(fw);
//            for (String value : index.get(colName).keySet()) {
//                bw.write(colName.concat("|").concat(value).concat("|").concat(index.get(colName).get(value).toString()).concat("\n"));
//            }
//            bw.close();
//            fw.close();
//        }
//
//    }

//    public HashMap<String, HashMap<String, ArrayList<String>>> getIndexOK() {
//        HashMap<String, HashMap<String, ArrayList<String>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
//
//        try {
//            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("indexes/" + this.getTableName().toUpperCase() + ".txt")),
//                    "UTF-8"));
//            String lineTxt = null;
//            while ((lineTxt = br.readLine()) != null) {
//                String[] record = lineTxt.split("\\|");
//                String col = record[0].toUpperCase();
//                String p = record[1];
//                String[] indseq = record[2].replace("null","").replace(" ", "").replace("[", "").replace("]", "").split(",");
//                ArrayList<String> list = new ArrayList<>(Arrays.asList(indseq));
//                if (index.containsKey(col)) {
//                    index.get(col).put(p, list);
//                } else {
//                    HashMap<String, ArrayList<String>> hashMap = new HashMap<>();
//                    hashMap.put(p, list);
//                    index.put(col, hashMap);
//                }
//            }
//            br.close();
//        } catch (Exception e) {
//            System.err.println("read errors :" + e);
//        }
//        return index;
//    }
//
//    public void setIndexTXTTotal() throws Exception {
//        //一张表生成一个文件
//        HashMap<String, HashMap<String, StringBuilder>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
//        List<Integer> attrIndex = new ArrayList<>();
//        if (tableName.equals("LINEITEM")) {
//            index.put("L_QUANTITY", new HashMap<>());
//            index.put("L_DISCOUNT", new HashMap<>());
//            index.put("L_RETURNFLAG", new HashMap<>());
//        }
//        if (tableName.equals("PART")){
//            index.put("P_SIZE", new HashMap<>());
//        }
//
//        for (int i = 0; i < primaryKey.size(); i++) {
//            index.put(primaryKey.get(i).getColumnName(), new HashMap<>());
//        }
//        for (int i = 0; i < references.size(); i++) {
//            index.put(references.get(i).getColumnName(), new HashMap<>());
//        }
//
//        for (int i = 0; i < columnInfo.size(); i++) {
//            if (index.containsKey(columnInfo.get(i).getColumnName()))
//                attrIndex.add(i);
//        }
//        int i = 1;
//        FileReader fs = new FileReader(fileDir);
//        BufferedReader br = new BufferedReader(fs);
//        String line;
//        if (index.size() != 0) {
//            while((line = br.readLine()) != null){
//                String[] tuple = line.split("\\|");
//                for (int j = 0; j < attrIndex.size(); j++) {
//                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
//                    String colName = columnInfo.get(attrIndex.get(j)).getColumnName();
//                    String attrVal = tuple[attrIndex.get(j)];
//                    if (!index.get(colName).containsKey(attrVal)) {
//                        index.get(colName).put(attrVal,new StringBuilder(String.valueOf(i)));
//                    } else {
//                        index.get(colName).put(attrVal, index.get(colName).get(attrVal).append(",").append(i));
//                    }
//                }
//
//                i++;
//            }
//        }
//        File file = new File("indexes/" + this.getTableName().toUpperCase() + ".txt");
//        if (!file.exists()) {
//            file.createNewFile();
//        }
//        FileWriter fw = new FileWriter(file, false);
//        BufferedWriter bw = new BufferedWriter(fw);
//        for (String column : index.keySet()) {
//            for (String value : index.get(column).keySet()) {
//                String record =column + "|" + value + "|" + index.get(column).get(value) + "\n";
//                bw.write(record);
//            }
//        }
//        bw.close();
//        fw.close();
//
//    }
//
//    public HashMap<String, ArrayList<String>> getIndex1(String ColName) {
//        final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + "_" + ColName + ".txt";
//        HashMap<String, ArrayList<String>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
//
//        try {
//            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(FILE_NAME)),
//                    "UTF-8"));
//            String lineTxt = null;
//            while ((lineTxt = br.readLine()) != null) {
//                String[] record = lineTxt.split("\\|");
//                String colVal = record[1].toUpperCase();
//                //String[] indseq = record[2].split(",");
//                String[] indseq = record[2].replace(" ", "").replace("[", "").replace("]", "").split(",");
//                ArrayList<String> list = new ArrayList<>(Arrays.asList(indseq));
//                index.put(colVal, list);
//
//            }
//            br.close();
//        } catch (Exception e) {
//            System.err.println("read errors :" + e);
//        }
//        return index;
//    }


    public boolean isLeft() {
        return isLeft;
    }

    public void setLeft(boolean left) {
        isLeft = left;
    }

    public List<Integer> getIndex() {
        return index;
    }

    public void setIndex(List<Integer> index) {
        this.index = index;
    }

    public void setIndexTXTDivide1(int part) throws Exception {

        //有划分，1列一文件
        for (int k = 0; k < part; k++) {
            HashMap<String, HashMap<String, StringBuilder>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
            List<Integer> attrIndex = new ArrayList<>();
            for (int i = k * columnInfo.size() / part; i < (k + 1) * columnInfo.size() / part; i++) {
                index.put(columnInfo.get(i).getColumnName(), new HashMap<>());
            }
            if (index.size() == 0) {
                continue;
            }
            for (int i = 0; i < columnInfo.size(); i++) {
                if (index.containsKey(columnInfo.get(i).getColumnName()))
                    attrIndex.add(i);
            }
            int i = 1;
            FileReader fs = new FileReader(fileDir);
            BufferedReader br = new BufferedReader(fs);
            String line;
            if (index.size() != 0) {
                while ((line = br.readLine()) != null) {
                    String[] tuple = line.split("\\|");
                    for (int j = 0; j < attrIndex.size(); j++) {
                        //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
                        String colName = columnInfo.get(attrIndex.get(j)).getColumnName();
                        String attrVal = tuple[attrIndex.get(j)];
                        if (!index.get(colName).containsKey(attrVal)) {
                            index.get(colName).put(attrVal, new StringBuilder(String.valueOf(i)));
                        } else {
                            index.get(colName).put(attrVal, index.get(colName).get(attrVal).append(",").append(i));
                        }
                    }
                    i++;
                }
            }
            br.close();
            for (String colName :index.keySet()){
                File file = new File("indexes/" + this.getTableName().toUpperCase()+"_"+colName + ".txt");
                if (!file.exists()) {
                    file.createNewFile();
                }
                FileOutputStream outputStream = new FileOutputStream(file);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                objectOutputStream.writeObject(index.get(colName));
                objectOutputStream.close();
            }
//            for (Iterator<Map.Entry<String, HashMap<String, StringBuilder>>> it = index.entrySet().iterator(); it.hasNext(); ) {
//                Map.Entry<String, HashMap<String, StringBuilder>> item = it.next();
//                String colName = item.getKey();
//                List<String> result;
//                HashMap<String, List<Integer>> col = new HashMap<>();
//                File file = new File("indexes/" + this.getTableName().toUpperCase() + "_" + colName + ".txt");
//                if (!file.exists()) {
//                    file.createNewFile();
//                }
//                FileOutputStream outputStream = new FileOutputStream(file);
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
//                List<String> colVals = new ArrayList<>();
//                colVals.addAll(index.get(colName).keySet());
//                for (String colVal : colVals) {
////                    result = Arrays.asList(index.get(colName).get(colVal).toString().split(","));
////                    index.get(colName).remove(colVal);
////                    List<Integer> a = result.stream().map(Integer::parseInt).collect(Collectors.toList());
//                    List<Integer> list = Arrays.stream(index.get(colName).get(colVal).toString().split(",")).map(Integer::parseInt).collect(Collectors.toList());
//                    col.put(colVal, list);
//                }
//                it.remove();
//                objectOutputStream.writeObject(col);
//                objectOutputStream.close();
//            }
        }
    }

    public void setIndexTXTDivide123(int part) throws Exception {
        Comparator c = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                // TODO Auto-generated method stub
                if (o1 < o2)
                    return -1;
                    //注意！！返回值必须是一对相反数，否则无效。jdk1.7以后就是这样。
                else return 1;
            }
        };
        //有划分，1列一文件
        for (int k =0;k<part;k++){
            HashMap<String, HashMap<String, List<Integer>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
            List<Integer> attrIndex = new ArrayList<>();
            for (int i = k*columnInfo.size()/part; i < (k+1)*columnInfo.size()/part; i++) {
                index.put(columnInfo.get(i).getColumnName(), new HashMap<>());
            }
            if (index.size()==0){
                continue;
            }
            for (int i = 0; i < columnInfo.size(); i++) {
                if (index.containsKey(columnInfo.get(i).getColumnName()))
                    attrIndex.add(i);
            }
            int i = 1;
            FileReader fs = new FileReader(fileDir);
            BufferedReader br = new BufferedReader(fs);
            String line;
            if (index.size() != 0) {
                while((line = br.readLine()) != null){
                    String[] tuple = line.split("\\|");
                    for (int j = 0; j < attrIndex.size(); j++) {
                        //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
                        String colName = columnInfo.get(attrIndex.get(j)).getColumnName();
                        String attrVal = tuple[attrIndex.get(j)];
                        if (!index.get(colName).containsKey(attrVal)) {
                            List<Integer> a = new LinkedList<>();
                            a.add(i);
                            index.get(colName).put(attrVal,a);
                        } else {
                            index.get(colName).get(attrVal).add(i);
                        }
                    }
                    i++;
                }
            }
            br.close();
            for (String colName :index.keySet()){
                File file = new File("indexes/" + this.getTableName().toUpperCase()+"_"+colName + ".txt");
                if (!file.exists()) {
                    file.createNewFile();
                }
                FileOutputStream outputStream = new FileOutputStream(file);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                objectOutputStream.writeObject(index.get(colName));
                objectOutputStream.close();
            }
        }}

    public HashMap<String, List<Integer>> getIndex(String ColName) throws Exception{
        final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + "_" + ColName + ".txt";
        FileInputStream inputStream = new FileInputStream(new File(FILE_NAME));//创建文件字节输出流对象
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        HashMap<String, List<Integer>> result = new HashMap<>();
        HashMap<String, StringBuilder> object = (HashMap<String, StringBuilder>)objectInputStream.readObject();
        List<String> colNames = new ArrayList<>();
        colNames.addAll(object.keySet());
        for (String colName:colNames) {
            result.put(colName, Arrays.stream(object.get(colName).toString().split(",")).map(Integer::parseInt).collect(Collectors.toList()));
            object.remove(colName);
        }
        return result;
    }

    public void BufferIndex()throws Exception{
        CSVFormat formator = CSVFormat.DEFAULT.withDelimiter('|');
        CSVParser csvParser = new CSVParser(new FileReader(fileDir), formator);
        Iterator<CSVRecord> iterator = csvParser.iterator();
        String tableName = this.getTableName();

        String fileName = "indexes/" + tableName+".txt";
        File file = new File(fileName);
        file.createNewFile();
        long start = System.currentTimeMillis();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);

        FileOutputStream outputStream = new FileOutputStream(file,false);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        int counter = 0;
        while (iterator.hasNext()){
            Tuple t = new Tuple(tableName,this.getColumnDefinitions(),iterator.next(),0);

            objectOutputStream.writeObject(t);
            counter+=1;
            if (counter==10000){
                objectOutputStream.flush();
                objectOutputStream.reset();
                counter=0;
            }
        }
        objectOutputStream.flush();
        objectOutputStream.close();
        long end = System.currentTimeMillis();
        logger.info(String.valueOf(end-start));

    }

    public void print() {
        Iterator<Tuple> iterator = this.getIterator();
        while (iterator.hasNext()) {
            iterator.next().printTuple(this.columnDefinitions, this.columnInfo);
            //iterator.next().printTuple(this.columnDefinitions, this.columnInfo);
        }
    }



}
