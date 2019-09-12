package edu.buffalo.www.cse4562.SQLparser;

import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class CreatParser {
    static Logger logger = Logger.getLogger(CreatParser.class.getName());

    public static HashMap<String, Object> CreatFunction(CreateTable create) {
        HashMap<String, Object> createMap = new HashMap<String, Object>();

        List<ColumnDefinition> colDefList = create.getColumnDefinitions();
        String tableName = create.getTable().getName();
        String alias = create.getTable().getAlias();
        String schemaName = create.getTable().getSchemaName();
        List<Index> indexList = create.getIndexes();
        List tblOptStr = create.getTableOptionsStrings();

        createMap.put("TABLENAME", tableName);
        if (colDefList != null) {
            for (int i = 0; i < colDefList.size(); i++) {
                createMap.put("COLNAME" + i, colDefList.get(i));
            }
        }
        if (alias != null) {
            createMap.put("ALIAS", alias);
        }
        return createMap;
    }

    public static boolean CreatFunction(CreateTable create, HashMap<String, TableObject> tableMap) throws Exception {
        String tableName = create.getTable().getName().toUpperCase();
        if (tableMap.containsKey(tableName)) {
            return false;
        }
        Table table = create.getTable();
        TableObject tableObject = new TableObject(create, table, tableName);
        tableMap.put(tableName, tableObject);
        logger.info(create.toString());
        File file = new File(tableObject.getFileDir());
        long length = file.length();
        if (length<8000000){
            tableObject.setIndexTXTDivide1(1);

            //tableObject.setIndexTXTDivide1(1);
        }else if (length<12000000){
            tableObject.setIndexTXTDivide1(3);
        }else if(length<16000000){
            tableObject.setIndexTXTDivide1(6);
        }else{
            tableObject.setIndexTXTDivide1(8);
        }
        return true;
    }

}
