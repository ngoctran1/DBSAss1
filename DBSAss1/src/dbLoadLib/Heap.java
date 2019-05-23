package dbLoadLib;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Heap {
	private RandomAccessFile heapFile;
	private int pageSize;
	private static int[] heapDataTypes = new int[]{LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.INT_TYPE, LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.INT_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.STR_TYPE, LineProcess.INT_TYPE,
			  LineProcess.STR_TYPE};
	
	public Heap(String heapFileName, int pageSize) {
		this.pageSize = pageSize;
		try {
			heapFile = new RandomAccessFile(heapFileName, "rw");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public Page getPage(int pageID) {
		byte[] data = new byte[pageSize];
		try {
			heapFile.seek(pageID * pageSize);
			heapFile.readFully(data);
		} catch (Exception e) {
			return null;
		}
		return new Page(data, heapDataTypes);
	}
	
	public Page getPage() {
		byte[] data = new byte[pageSize];
		try {
			heapFile.readFully(data);
		} catch (Exception e) {
			return null;
		}
		return new Page(data, heapDataTypes);
	}
}
