package edu.buffalo.www.cse4562.SQLparser;

import edu.buffalo.www.cse4562.Evaluate.projectionEval;
import edu.buffalo.www.cse4562.Evaluate.selectionEval;
import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class SelectParser {
    /*
    implement 2 ways to parse SQL
     */
    private PlainSelect body;
    private List<Join> joins;
    private List<SelectItem> selectItem;
    private Expression where;
    private List<OrderByElement> orderBy;
    private Distinct dist;
    private Limit lim;
    private FromItem fromItem;
    private List<net.sf.jsqlparser.schema.Column> groupByColumnReference;

    public SelectParser(SelectBody selectBody){

        this.body = (PlainSelect) selectBody;
        this.joins = body.getJoins();
        this.selectItem = body.getSelectItems();
        this.where = body.getWhere();
        this.orderBy = body.getOrderByElements();
        this.dist = body.getDistinct();
        this.lim = body.getLimit();
        this.fromItem = body.getFromItem();
        this.groupByColumnReference = body.getGroupByColumnReferences();
    }
    static Logger logger = Logger.getLogger(SelectItem.class.getName());

    /**
     * Parse the SelectBody into HashMap
     *
     * @param body
     * @param tableCounter
     * @return
     */
    public static HashMap<String, Object> SelectFunction(SelectBody body, int tableCounter) {

        HashMap<String, Object> query = new HashMap<>();
        HashMap<String, Object> operations = new HashMap<>();

        if (!(body instanceof Union)) {
            // get required elements
            List<Join> joins = ((PlainSelect) body).getJoins();
            List<SelectItem> selectItem = ((PlainSelect) body).getSelectItems();
            Expression where = ((PlainSelect) body).getWhere();
            List orderby = ((PlainSelect) body).getOrderByElements();
            Distinct dist = ((PlainSelect) body).getDistinct();
            Limit lim = ((PlainSelect) body).getLimit();
            FromItem fromItem = ((PlainSelect) body).getFromItem();
            String tableAlias = fromItem.getAlias();

            tableCounter = solveFromList(query, fromItem, tableCounter, tableAlias);

            if (joins != null) {
                for (int i = 0; i < joins.size(); i++) {
                    tableCounter = solveFromList(query, joins.get(i).getRightItem(), tableCounter, joins.get(i).getRightItem().getAlias());
                }
            }

            if (where != null) {
                operations.put("WHERE", where);
            }

            //Select Item
            if (selectItem != null) {
                //The select list must be traversed during the data process
                //Do not need to parse the select list here,
                operations.put("SELECTITEM", selectItem);
            }

            if (orderby != null) {
                operations.put("ORDERBY", orderby);
            }
            if (dist != null) {
                operations.put("DISTINCT", dist);
            }
            if (lim != null) {
                operations.put("LIMIT", lim);
            }

            query.put("OPERATIONS", operations);
        } else {
            //todo UNION
            int unionSize = ((Union) body).getPlainSelects().size();
            for (int i = 0; i < unionSize; i++) {
                PlainSelect sub = ((Union) body).getPlainSelects().get(i);
                //query.put("Union",SelectFunction(sub,tableCounter));
            }
        }
        return query;
    }

    public static int solveFromList(HashMap<String, Object> query, FromItem fromItem, int tableCounter, String tableAlias) {
        if (fromItem instanceof SubSelect) {
            //1 subselect
            SubSelect sub = ((SubSelect) fromItem);
            String subAlias = sub.getAlias();
            if (subAlias == null) {
                query.put("FromsubSelect" + tableCounter, SelectFunction(sub.getSelectBody(), tableCounter));
            } else {
                query.put(subAlias, SelectFunction(sub.getSelectBody(), tableCounter));
            }
        } else {
            //0 subselect
            if (tableAlias == null) {
                query.put("fromSelect" + tableCounter++, fromItem);
            } else {
                query.put(tableAlias, fromItem);
                tableCounter++;
            }
        }
        return tableCounter;
    }

    /**
     * Parse the SelectBody into RA tree
     *
     * @param body
     * @return the root node
     */
    public RANode SelectFunction(SelectBody body,HashMap<String, TableObject> tableMap) {
        List<TableObject> involvedTables = new ArrayList<>();

        if (!(body instanceof Union)) {
            //parse the SQL and build the tree down to up
            RANode joinNode = new RAJoin(fromItem);
            RANode pointer = joinNode;

            if (fromItem instanceof SubSelect) {
                // subSelect
                SelectParser subParser = new SelectParser(((SubSelect) fromItem).getSelectBody());
                RANode subSelectBody = subParser.SelectFunction(((SubSelect) fromItem).getSelectBody(),tableMap);
                joinNode.setLeftNode(subSelectBody);
                subSelectBody.setParentNode(pointer);
            } else {
                //table
                RATable table = new RATable((Table) fromItem);
                joinNode.setLeftNode(table);
                table.setParentNode(pointer);
                String tableName= table.getTable().getName().toUpperCase();
                involvedTables.add(new TableObject(tableMap.get(tableName),table,fromItem.getAlias()));
            }

            if (joins != null) {
                for (int i = 0; i < joins.size(); i++) {
                    FromItem join = joins.get(i).getRightItem();
                    // process SubSelect
                    if (join instanceof SubSelect) {
                        if (joinNode.getRightNode() == null) {
                            //     join
                            //    /   \
                            //        null
                            RANode subSelectBody = SelectFunction(((SubSelect) join).getSelectBody(),tableMap);
                            joinNode.setRightNode(subSelectBody);
                            subSelectBody.setParentNode(pointer);
                        } else {
                            //if both children are not empty, new a new RAjoin node , insert the node into leftchild position
                            //     join
                            //    /   \
                            //        somethings
                            RANode joinNew = new RAJoin(fromItem);
                            while (joinNode.getLeftNode() != null) {
                                joinNode = joinNode.getLeftNode();
                            }
                            pointer.setParentNode(joinNew);
                            joinNew.setLeftNode(pointer);
                            pointer = joinNew;
                            RANode subSelect = SelectFunction(((SubSelect) join).getSelectBody(),tableMap);
                            pointer.setRightNode(subSelect);
                            subSelect.setParentNode(pointer);

                        }
                    } else {
                        // not SubSelect
                        if (joinNode.getRightNode() == null) {
                            //     join
                            //    /   \
                            //        null
                            RATable table = new RATable((Table) join);
                            joinNode.setRightNode(table);
                            table.setParentNode(pointer);
                            String tableName= table.getTable().getName().toUpperCase();
                            involvedTables.add(new TableObject(tableMap.get(tableName),table,join.getAlias()));
                        } else {
                            //if both children are not empty, new a new RAjoin node , insert the node into leftchild position
                            //           join
                            //          /   \
                            // add join       null
                            RANode joinNew = new RAJoin(fromItem);
                            pointer.setParentNode(joinNew);
                            joinNew.setLeftNode(pointer);
                            pointer = joinNew;
                            RATable table = new RATable((Table) join);
                            pointer.setRightNode(table);
                            table.setParentNode(pointer);
                            String tableName= table.getTable().getName().toUpperCase();
                            involvedTables.add(new TableObject(tableMap.get(tableName),table,join.getAlias()));
                        }

                    }
                }
            }
            //****************************Optimize******************************//
            projectionEval projEval = new projectionEval(selectItem);
            //find the columns which will be used
            List<Column> columnList = projEval.usefulCol(where,orderBy,groupByColumnReference,involvedTables);
            //find the first JOIN Node
            RANode joinRoot = pointer;
            while (!joinRoot.getOperation().equals("JOIN")){
                joinRoot = joinRoot.getLeftNode();
            }
            //todo Select A match the table
            projEval.pushdownProject(joinRoot,columnList,involvedTables);

            if (where!=null&&joins!=null){
                //pushdown selection
                selectionEval selectEval = new selectionEval();
                selectEval.pushdownSelect(pointer,where,involvedTables);
                this.where = selectEval.getWhere();
            }
            //****************************Optimize******************************//

            //process where
            if (where != null) {
                RANode whereNode = new RASelection(where);
                whereNode.setLeftNode(pointer);
                pointer.setParentNode(whereNode);
                pointer = whereNode;
            }

            //process groupBy
            if (groupByColumnReference!=null){
                RAGroupBy groupNode = new RAGroupBy(groupByColumnReference,selectItem);
                groupNode.setLeftNode(pointer);
                pointer.setParentNode(groupNode);
                pointer = groupNode;
            }

            //process projection
            RANode projNode = new RAProjection(selectItem);
            // if groupby exist, when process where ,the result should be hash and groupBy to reduce time cost.
            if (groupByColumnReference!=null){
                ((RAProjection)projNode).setisGroupBy(true);
            }
            projNode.setLeftNode(pointer);
            pointer.setParentNode(projNode);
            pointer = projNode;

            //process orderby
            if (orderBy != null) {
                RANode orderbyNode = new RAOrderby(orderBy);
                orderbyNode.setLeftNode(pointer);
                pointer.setParentNode(orderbyNode);
                pointer = orderbyNode;
            }

            //process distinct
            if (dist != null) {
                RANode distNode = new RADistinct(dist);
                distNode.setLeftNode(pointer);
                pointer.setParentNode(distNode);
                pointer = distNode;
            }

            //process limit
            if (lim != null) {
                RANode limNode = new RALimit(lim);
                limNode.setLeftNode(pointer);
                pointer.setParentNode(limNode);
                pointer = limNode;
            }
            return pointer;
        } else {
            return null;
        }
    }
}
