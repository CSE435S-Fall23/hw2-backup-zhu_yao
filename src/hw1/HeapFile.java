package hw1;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	
	private File file;
	private TupleDesc tupleDesc;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		this.file = f;
		this.tupleDesc = type;
	}
	
	public File getFile() {
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		return this.tupleDesc;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 * @throws IOException 
	 */
	public HeapPage readPage(int id) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(this.file, "rws");
		byte[] data = new byte[PAGE_SIZE];
		raf.seek(id*(PAGE_SIZE));
		raf.read(data);
		HeapPage p = new HeapPage(id, data, this.getId());
		return p;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		return this.file.hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws IOException 
	 */
	public void writePage(HeapPage p) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
		byte[] data = p.getPageData();
		raf.seek(PAGE_SIZE * p.getId());
		raf.write(data);
		raf.close();
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception 
	 */
	public HeapPage addTuple(Tuple t) throws Exception {
		HeapPage newPage = null;
		boolean full = true;
		
		for(int i = 0; i < this.getNumPages(); i++) {
			newPage = this.readPage(i);
			for(int j =0; j < newPage.getNumSlots(); ++j) {
				if(!newPage.slotOccupied(j)) {
					full = false;
					newPage.addTuple(t);
					this.writePage(newPage);
					return newPage;	
				}
			}
		}
		
		// consider the case where pages are full
		if(full) {
			newPage = new HeapPage(this.getNumPages(), new byte[PAGE_SIZE], this.getId());
			newPage.addTuple(t);
			this.writePage(newPage);
			return newPage;
		}
		return null;

	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 * @throws Exception 
	 */
	public void deleteTuple(Tuple t) throws Exception{
		HeapPage newPage = this.readPage(t.getPid());
		newPage.deleteTuple(t);
		this.writePage(newPage);
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 * @throws IOException 
	 */
	public ArrayList<Tuple> getAllTuples() throws IOException {
		ArrayList<Tuple> allTuples = new ArrayList<>();
		for(int i = 0; i < this.getNumPages(); ++i) {
			HeapPage newPage = this.readPage(i);
			Iterator<Tuple> iterator = newPage.iterator();
			while(iterator.hasNext()){
				allTuples.add((Tuple)iterator.next());
			}
		}
		return allTuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		int totalPages = (int) file.length() /(PAGE_SIZE);
		return totalPages;
	}
}
