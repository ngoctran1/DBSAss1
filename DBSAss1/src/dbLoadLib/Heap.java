package dbLoadLib;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class Heap {
	private static final int MAX_NUM_PAGES_STORED = 1000;
	
	private RandomAccessFile heapFile;
	private int pageSize;
	private HashMap<Integer, Integer> pageDirectory = null;
	private Page[] pages = null;
	private int[] pageIDs = null;
	private int numPagesPulled = 0;
	private int currentPage = 0;
	
	private static int[] heapDataTypes = new int[]{LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.INT_TYPE, LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.INT_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.STR_TYPE, LineProcess.INT_TYPE,
			  LineProcess.STR_TYPE};
	
	public Heap(String heapFileName, int pageSize) {
		this.pageSize = pageSize;
		pageDirectory = new HashMap<>();
		pages = new Page[MAX_NUM_PAGES_STORED];
		pageIDs = new int[MAX_NUM_PAGES_STORED];
		try {
			heapFile = new RandomAccessFile(heapFileName, "rw");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	// Reads next page if pageID == -1, else pulls page at PageID (either from memory or DB)
	public Page getPage(int pageID) {
		byte[] data = new byte[pageSize];
		Page newPage;
		
		// Check if page is already in memory
		if(pageDirectory.containsKey(pageID)) {
			return pages[pageDirectory.get(pageID)];
		}
		
		// Read in page from heap
		try {
			if(pageID > -1) {
				heapFile.seek(pageID * pageSize);
			}
			heapFile.readFully(data);
			numPagesPulled++;
		} catch (IOException e) {
			return null;
		}
		newPage = new Page(data, heapDataTypes);
		
		if(pageID > -1) {
			// Store page in memory (remove oldest set page if no more space)
			if(numPagesPulled > MAX_NUM_PAGES_STORED) {
				pageDirectory.remove(pageIDs[currentPage]);
			}
			
			pages[currentPage] = newPage;
			pageIDs[currentPage] = pageID;
			pageDirectory.put(pageID, currentPage);
			currentPage = (currentPage + 1) % pages.length;
		}
		return newPage;
	}
	
	public int getNumPagesPulled() {
		return numPagesPulled;
	}
}
