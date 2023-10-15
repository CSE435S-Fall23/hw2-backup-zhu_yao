package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		  ArrayList<Tuple> tuples_select = new ArrayList<>();
		  
		  for(int i = 0; i < tuples.size(); ++i) {
		   Tuple temp_tuple = tuples.get(i);
		   if(temp_tuple.getField(field).compare(op, operand)) {
		    tuples_select.add(temp_tuple);
		   }
		  }
		 
		  return new Relation(tuples_select, this.td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		  Type[] type_temp = this.td.getAllTypes();
		  String[] field_temp = this.td.getAllFields();
		  for(int i=0; i < fields.size(); ++i) {
			  int field = fields.get(i);
			  if(td.getFieldName(field)!=names.get(i) && names.get(i)!=null && fields.get(i)!= null) {
				  field_temp[i] = names.get(i);
			  }
		  }
		  TupleDesc td_new = new TupleDesc(type_temp, field_temp);
		  return new Relation(tuples,td_new);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	
	public Relation project(ArrayList<Integer> fields) {
		ArrayList<Tuple> projectedTuples = new ArrayList<>();
	    
		for(Tuple tuple: this.tuples) {
			Tuple newTuple = new Tuple(td.project(fields));
			Field[] selectedFields = tuple.getProjectedFields(fields);
			for(int i = 0; i < selectedFields.length; i++) {
				newTuple.setField(i, selectedFields[i]);
			}
			projectedTuples.add(newTuple);
			}
		return new Relation(projectedTuples, td.project(fields));

		}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		ArrayList<Tuple> joinedTuples = new ArrayList<>();
		for (Tuple tuple1 : this.tuples) {
	        for (Tuple tuple2 : other.tuples) {
	            if (tuple1.getField(field1).equals(tuple2.getField(field2))) {
	                Tuple newTuple = concatenateTuples(tuple1, tuple2);
	                joinedTuples.add(newTuple);
	            }
	        }
	    }
	    TupleDesc newDesc = concatenateTupleDescs(this.td, other.td);
	    System.out.print(newDesc.getSize());
		return new Relation(joinedTuples, newDesc);
	}
	
	private TupleDesc concatenateTupleDescs(TupleDesc td1, TupleDesc td2) {
	    Type[] concatenatedTypes = new Type[td1.numFields() + td2.numFields()];
	    String[] concatenatedFieldNames = new String[td1.numFields() + td2.numFields()];
	    
	    for(int i = 0; i < td1.numFields(); i++) {
	    	concatenatedTypes[i] =  td1.getType(i);
	    	concatenatedFieldNames[i] = td1.getFieldName(i);
	    }
	    
	    for(int i = 0; i < td2.numFields(); i++) {
	    	concatenatedTypes[td1.numFields()+i] =  td2.getType(i);
	    	concatenatedFieldNames[td1.numFields()+i] = td2.getFieldName(i);
	    }
	    
	    return new TupleDesc(concatenatedTypes, concatenatedFieldNames);
	}
	
	private Tuple concatenateTuples(Tuple t1, Tuple t2) {
	    Tuple newTuple = new Tuple(concatenateTupleDescs(t1.getDesc(),t2.getDesc()));
	    for(int i = 0; i < t1.getDesc().numFields(); i++) {
	    	newTuple.setField(i, t1.getField(i));
	    }
	    for(int i = 0; i < t2.getDesc().numFields(); i++) {
	    	newTuple.setField(t1.getDesc().numFields()+i, t2.getField(i));
	    }
	    return newTuple;
	}
	
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		return null;
	}
	
	public TupleDesc getDesc() {
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		return null;
	}
}
