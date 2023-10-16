package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.IntField;
import hw1.Query;
import hw1.Relation;



public class MyUnitTest {
	
	private Catalog c;
	@Before
	public void setup() {
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
	}
	
	
	
	@Test
	public void testAS() throws IOException {
		Query q = new Query("SELECT a2 AS Test FROM A AS RenameA");
		Relation r = q.execute();
		
		assert(r.getDesc().getSize() == 4);
		assert(r.getTuples().size() == 8);
		assert(r.getDesc().getFieldName(0).equals("Test"));
		int tableID = c.getTableId("RenameA");
		assertTrue("Catalog does not get table names correctly", c.getTableName(tableID).equals("RenameA"));

	}
	
	@Test
	public void testASInJoin() throws IOException {
		Query q = new Query("SELECT c1, c2, a1, a2 FROM test JOIN A AS K ON test.c1 = k.a1");
		Relation r = q.execute();
		assert(r.getTuples().size() == 5);
		assert(r.getDesc().getSize() == 141);
	}
	
}
