package hw1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute() throws IOException  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();

		
		// FROM clause
		FromItem fromItem = sb.getFromItem();
		Catalog c = Database.getCatalog();
		
	    String tableName = ((Table) fromItem).getName();  // Assuming it's always a Table for simplicity
	    HeapFile heapFile = c.getDbFile(c.getTableId(tableName));
	    TupleDesc tupleDesc = c.getTupleDesc(c.getTableId(tableName));
	    ArrayList<Tuple> mainTableTuples = heapFile.getAllTuples();
	    Relation currentRelation = new Relation(mainTableTuples, tupleDesc);
	    
	    // JOIN in the query 
	    List<Join> joins = sb.getJoins();
	    if (joins != null) {
	        for (Join join : joins) {
	            // Extract table and field info for join
	        	Table joinTable = (Table) join.getRightItem();
	        	String joinTableName = joinTable.getName();
	        	HeapFile joinHeapFile = c.getDbFile(c.getTableId(joinTableName));
	        	TupleDesc joinTupleDesc = c.getTupleDesc(c.getTableId(joinTableName));
	        	// Extract join condition (assuming it's an EqualsTo expression for simplicity)
	        	EqualsTo onExpression = (EqualsTo) join.getOnExpression();
	        	Column leftColumn = (Column) onExpression.getLeftExpression();
	        	Column rightColumn = (Column) onExpression.getRightExpression();

	        	int field1 = currentRelation.getDesc().nameToId(leftColumn.getColumnName());
	        	int field2 = joinTupleDesc.nameToId(rightColumn.getColumnName());

	        	// Perform the join
	        	currentRelation = currentRelation.join(new Relation(joinHeapFile.getAllTuples(), joinTupleDesc), field1, field2); 
	        }
	    }
	   
	    // WHERE clause
	    Expression where = sb.getWhere();
	    if (where != null) {
	        WhereExpressionVisitor wev = new WhereExpressionVisitor();
	        where.accept(wev);
	        // Extract the column name, operator, and value from the WHERE clause
	        String columnName = wev.getLeft();
	        Field value = wev.getRight();
	        RelationalOperator op = wev.getOp();
            int fieldIndex = tupleDesc.nameToId(columnName);
            currentRelation = currentRelation.select(fieldIndex, op, value);
	    }
	    
	   
	    // SELECT clause
	    List<SelectItem> selectItems = sb.getSelectItems();
	    ArrayList<Integer> fieldsToProject = new ArrayList<>();
        ColumnVisitor colVisitor = new ColumnVisitor();
	    
	    for (SelectItem si : selectItems) {
	        si.accept(colVisitor);
	        
	        String colName = colVisitor.getColumn(); 
	        if (colName.equals("*")) {
                for(int i = 0; i<currentRelation.getDesc().numFields(); i++) {
                	fieldsToProject.add(i);
                }
                break;
            }
	        int fieldNum = currentRelation.getDesc().nameToId(colName);
	        if(!fieldsToProject.contains(fieldNum)) fieldsToProject.add(fieldNum);
	    }
	    
        currentRelation = currentRelation.project(fieldsToProject);

	    
	    // Aggregate
	    boolean hasGroupBy = (sb.getGroupByColumnReferences() != null);
	    if (colVisitor.isAggregate()) {
	    	currentRelation = currentRelation.aggregate(colVisitor.getOp(), hasGroupBy);
	    }
	    
	    // GROUP BY
//	    List<Expression> groupByColumns = sb.getGroupByColumnReferences();
//	    // Determine the GROUP BY fields
//	    ArrayList<Integer> groupByFields = new ArrayList<>();
//
//	    if (groupByColumns != null) {
//	        // Explicit GROUP BY
//	        for (Expression column : groupByColumns) {
//	            groupByFields.add(currentRelation.getDesc().nameToId(column.toString()));
//	        }
//	    } else if (!aggregateFields.isEmpty() && !nonAggregateFields.isEmpty()) {
//	        // Implicit GROUP BY based on non-aggregate fields
//	        groupByFields = nonAggregateFields; 
//	    }
//
//	    // Apply GROUP BY
//	    if (!groupByFields.isEmpty()) {
//	        currentRelation = currentRelation.groupBy(groupByFields);
//	    }
//
//	    // Apply aggregate operations
//	    if (!aggregateOps.isEmpty()) {
//	        for (AggregateOperator op : aggregateOps) {
//	            currentRelation = currentRelation.aggregate(op, true); 
//	        }
//	    } else {
//	        // If no aggregate functions, just project
//	        currentRelation = currentRelation.project(fieldsToProject);
//	    }

		
		return currentRelation;
		
	}
	
}
