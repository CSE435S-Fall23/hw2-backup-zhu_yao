package hw1;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	
	/**
	 * Specify the variables for the Tuple 
	 * @param tupleDesc the schema for the instance
	 * @param fields array that contains the field information
	 * @param pID stores the pageID of the tuple
	 * @param sID stores the slotID of the tuple
	 */
	
	private Field[] fields;
	private TupleDesc tupleDesc;
	private int pID;
	private int sID;
	
	public Tuple(TupleDesc t) {
		this.tupleDesc = t;
		this.fields = new Field[t.numFields()];
	}
	
	public TupleDesc getDesc() {
		return this.tupleDesc;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {

		return this.pID;
	}

	public void setPid(int pid) {
		this.pID = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		
		return this.sID;
	}

	public void setId(int id) {
		this.sID = id;
	}
	
	public void setDesc(TupleDesc td) {
		this.tupleDesc = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		fields[i] = v;
	}
	
	public int getFieldLen() {
		return this.fields.length;
	}
	
	public Field[] getAllField() {
		return this.fields;
	}
	
	public Field getField(int i) {
		
		return fields[i];
	}
	
	public Field[] getProjectedFields(ArrayList<Integer> fieldIndices) {
	    Field[] projectedFields = new Field[fieldIndices.size()];
	    for (int i = 0; i < fieldIndices.size(); i++) {
	        projectedFields[i] = this.fields[fieldIndices.get(i)];
	    }
	    return projectedFields;
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		StringBuilder stringRep = new StringBuilder();
		for (int i = 0; i < fields.length; ++i) {
			stringRep.append(fields[i].toString());
			stringRep.append(" ");
		}
		return stringRep.toString();
	}
}
	
