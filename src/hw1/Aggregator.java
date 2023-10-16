package hw1;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	private AggregateOperator agg_op;
	private boolean groupBy;
	private TupleDesc td;
	private ArrayList<Tuple> tuples_current;
	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.agg_op = o;
		this.groupBy = groupBy;
		this.td = td;
		this.tuples_current = new ArrayList<Tuple>();	
		
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		// separate cases for each operation
		if(this.agg_op == AggregateOperator.MAX) {
			if(this.groupBy) {
				tuples_current.add(t);
			}
			else {
				if(this.tuples_current.size() == 0) {
					this.tuples_current.add(t);
				}
				else {
					if(t.getField(0).compare((RelationalOperator.GTE), tuples_current.get(0).getField(0))){
						tuples_current.set(0, t);
					}
				}
			}
		}
		
		else if(this.agg_op == AggregateOperator.MIN) {
			if(this.groupBy) {
				tuples_current.add(t);
			}
			else {
				if(this.tuples_current.size() == 0) {
					this.tuples_current.add(t);
				}
				else {
					if(t.getField(0).compare((RelationalOperator.LT), tuples_current.get(0).getField(0))){
						tuples_current.set(0, t);
					}
				}
			}
		}
		
		else if(this.agg_op == AggregateOperator.AVG){
			tuples_current.add(t);
		}
		
		else if(this.agg_op == AggregateOperator.COUNT) {
			tuples_current.add(t);
		}
		else if(this.agg_op == AggregateOperator.SUM){
			tuples_current.add(t);
		}
		
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		if(this.agg_op == AggregateOperator.MAX){
			if(this.groupBy) {
				Type[] types = {this.td.getType(0), this.td.getType(1)};
				String[] names = {this.td.getFieldName(0), "MAX"};
				TupleDesc td_new = new TupleDesc(types,names);
				Map<Field, Field> agg_new = new HashMap<Field, Field>();
				for(Tuple t: tuples_current) {
					if(agg_new.containsKey(t.getField(0))) {
						if(agg_new.get(t.getField(0).compare(RelationalOperator.GTE, t.getField(1))) != null) {
						}
						else{
							agg_new.put(t.getField(0),t.getField(1));
						}	
					}
					else {
						agg_new.put(t.getField(0), t.getField(1));
					}
				}
				for(Field key: agg_new.keySet()) {
					Tuple tuple_new = new Tuple(td_new);
					tuple_new.setField(0, key);
					tuple_new.setField(1, agg_new.get(key));
					results.add(tuple_new);
				}
				return results;
			}
			else {
				return this.tuples_current;
			}
		}
		
		if(this.agg_op == AggregateOperator.MIN){
			if(this.groupBy) {
				Type[] types = {this.td.getType(0), this.td.getType(1)};
				String[] names = {this.td.getFieldName(0), "MIN"};
				TupleDesc td_new = new TupleDesc(types,names);
				Map<Field, Field> agg_new = new HashMap<Field, Field>();
				for(Tuple t: tuples_current) {
					if(agg_new.containsKey(t.getField(0))) {
						if(agg_new.get(t.getField(0).compare(RelationalOperator.LT, t.getField(1))) != null) {
						}
						else{
							agg_new.put(t.getField(0),t.getField(1));
						}	
					}
					else {
						agg_new.put(t.getField(0), t.getField(1));
					}
				}
				for(Field key: agg_new.keySet()) {
					Tuple tuple_new = new Tuple(td_new);
					tuple_new.setField(0, key);
					tuple_new.setField(1, agg_new.get(key));
					results.add(tuple_new);
				}
				return results;
			}
			else {
				return this.tuples_current;
			}
		}
		
		else if(this.agg_op == AggregateOperator.AVG) {
			if(this.groupBy) {
				String[] field_name = {this.td.getFieldName(0), "AVG"};
				Type[] field_type = {this.td.getType(0), this.td.getType(1)};
				TupleDesc new_td = new TupleDesc(field_type, field_name);
				
				if(this.td.getType(1) != Type.INT) {
					return null; // can not perform on string
				}
				
				ArrayList<Field> checked = new ArrayList<Field>();
				
				for(Tuple t : this.tuples_current) {
					if(!checked.contains(t.getField(0))) {
						checked.add(t.getField(0));
						int temp_sum = 0;
						int temp_count = 0;
						for(Tuple t2 : this.tuples_current) {
							if(t2.getField(0).compare(RelationalOperator.EQ, t.getField(0))) {
								temp_sum += t2.getField(1).hashCode();
								temp_count ++;
							}
						}
						int temp_result = temp_sum / temp_count; 
						Tuple new_tuple = new Tuple(new_td);
						IntField ans = new IntField(temp_result);
						new_tuple.setField(0, t.getField(0));
						new_tuple.setField(1, ans);
						results.add(new_tuple);
					}
					else {
						 }
					}
				
				return results;
				}
			else {
				if(this.td.getType(0) != Type.INT) {
					return null;
				}
				int sum = 0;
				int count = 0;
				for(int i = 0; i < this.tuples_current.size(); i++) {
					sum += this.tuples_current.get(i).getField(0).hashCode();
					count += 1;
				}
				int average_result = sum / count;
				
				Tuple new_tuple = new Tuple(this.td);
				IntField ans = new IntField(average_result);
				new_tuple.setField(0, ans);
				results.add(new_tuple);
				return results;
			}
		}
		
		
		
		
		else if(this.agg_op == AggregateOperator.COUNT) {
			if(this.groupBy) {
				String[] field_name = {this.td.getFieldName(0), "COUNT"};
				Type[] field_type = {this.td.getType(0), this.td.getType(1)};
				TupleDesc new_td = new TupleDesc(field_type, field_name);
				ArrayList<Field> checked = new ArrayList<Field>();
				
				for(Tuple t : this.tuples_current) {
					if(!checked.contains(t.getField(0))) {

						checked.add(t.getField(0));
						int temp_count = 0;
						for(Tuple t2 : this.tuples_current) {

							if(t2.getField(0).compare(RelationalOperator.EQ, t.getField(0))) {
								temp_count ++;
							}
						}
						Tuple new_tuple = new Tuple(new_td);
						IntField ans = new IntField(temp_count);
						new_tuple.setField(0, t.getField(0));
						new_tuple.setField(1, ans);
						results.add(new_tuple);
					}
					else {

						 }
					}
				
				return results;
				}
			else {
				Type[] field_types = new Type[] { Type.INT };
				String[] field_name = new String[] { "Count" };
				TupleDesc new_td = new TupleDesc(field_types, field_name);
				Tuple new_tuple = new Tuple(new_td);
				new_tuple.setField(0, new IntField(this.tuples_current.size()) );
				results.add(new_tuple);
				return results;
			}
		}
		
		//END COUNT
		
		else if(this.agg_op == AggregateOperator.SUM) {
			if(this.groupBy) {
				String[] field_name = {this.td.getFieldName(0), "SUM"};
				Type[] field_type = {this.td.getType(0), this.td.getType(1)};
				TupleDesc new_td = new TupleDesc(field_type, field_name);
				int sum = 0;
				ArrayList<Field> checked = new ArrayList<Field>();
				for(Tuple t : this.tuples_current) {
					if(!checked.contains(t.getField(0))) {
						checked.add(t.getField(0));
						int temp_sum = 0;
						for(Tuple t2 : this.tuples_current) {
							if(t2.getField(0).compare(RelationalOperator.EQ, t.getField(0))) {
								temp_sum += t2.getField(1).hashCode();
							}
						}
						Tuple new_tuple = new Tuple(new_td);
						IntField ans = new IntField(temp_sum);
						new_tuple.setField(0, t.getField(0));
						new_tuple.setField(1, ans);
						results.add(new_tuple);
					}
					else {
						 }
					}
				
				return results;
			}
			else {
				if(this.td.getType(0) != Type.INT) {
					return null;
				}
				int sum = 0;
				for(Tuple t : this.tuples_current) {
					sum += t.getField(0).hashCode();
				}
				Type[] field_types = new Type[] { Type.INT };
				String[] field_name = new String[] { "SUM" };
				TupleDesc new_td = new TupleDesc(field_types, field_name);
				Tuple new_tuple = new Tuple(new_td);
				new_tuple.setField(0, new IntField(sum));
				results.add(new_tuple);
				return results;
			}
		}
		
		return null;
	}
	}




