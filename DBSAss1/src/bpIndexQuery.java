import java.io.*;

import bpIndexLib.bpTree;
import dbLoadLib.Heap;

public class bpIndexQuery {
	private static final int MAX_ARGS = 5;
	private static final int MIN_ARGS = 3;
	private static final String NO_HEAP_FLAG = "-no-heap";
	private static final String RESULT_FILE = "bpIndexQuery-Result.txt";
	
	public static void main(String[] args) {
		long beginTime;
		long endTime;
		
		String query = null;
		String query2 = null;
		
		RandomAccessFile bpFile = null;
		String bpFileName = null;
		
		String heapFileName = null;
		int heapPageSize = -1;
		Heap heap = null;
		
		BufferedWriter resultOutput = null;
		
		bpTree tree = null;
		boolean equalitySearch = true;
		boolean readDB = true;
		
		// Parse arguments
		if(args.length <= MAX_ARGS && args.length >= MIN_ARGS) {
			if(args[0].compareTo(NO_HEAP_FLAG) == 0) {
				readDB = false;
				bpFileName = args[1];
				query = args[2];
				
				if(args.length == MIN_ARGS + 1) {
					equalitySearch = false;
					query2 = args[3];
				}
			} else if (args.length >= MIN_ARGS + 1){
				heapFileName = args[0];
				bpFileName = args[2];
				query = args[3];
				
				if(args.length == MAX_ARGS) {
					equalitySearch = false;
					query2 = args[4];
				}
				try {
					heapPageSize = Integer.parseInt(args[1]);
				} catch (NumberFormatException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
		} else {
			printCommandError();
			System.exit(1);
		}
		
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("INPUTS\n");
		System.err.println("Heap File: " + heapFileName);
		System.err.println("Heap Page Size: " + heapPageSize);
		System.err.println("B+ File: " + bpFileName);

		if(equalitySearch) {
			System.err.println("Query (Equality): " + query);
		} else {
			System.err.println("Query (Range): \"" + query + "\" - \"" + query2 + "\"");
		}
		System.err.println("--------------------------------------------------------------------------------");
		beginTime = System.nanoTime();
		try {
			bpFile = new RandomAccessFile(bpFileName, "r");
			resultOutput = new BufferedWriter(new FileWriter(RESULT_FILE));
			tree = new bpTree(bpFile);
			
			if(readDB) {
				heap = new Heap(heapFileName, heapPageSize);
			}
			
			System.err.println("--------------------------------------------------------------------------------");
			System.err.println("SEARCHING...\n");
			if(equalitySearch) {
				tree.equalityQuery(query, true, readDB, heap, resultOutput);
			} else {
				tree.rangeQuery(query, query2, readDB, heap, resultOutput);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			resultOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		tree.close();
		
		System.err.println("Results printed to file: " + RESULT_FILE);
		endTime = System.nanoTime();
		
		System.err.println("Num Leaves Read: " + tree.getNumLeavesRead());
		if(readDB) {
			System.err.println("Num Pages Pulled: " + heap.getNumPagesPulled());
		} else {
			System.err.println("Num Pages Pulled: 0 (No heap file loaded)");
		}
		
		long totalTime = endTime - beginTime;
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("SUMMARY STATS\n");
		System.err.printf("Heap File Read Time: %d ms\n", tree.getHeapFileReadTime() / 1000000);
		System.err.printf("Total Time Taken: %d ms\n", totalTime / 1000000);
	}
	
	private static void printCommandError() {
		System.err.println("Invalid parameters. Please use the following:\n");
		System.err.println("Equality Query: java bpIndexQuery <Heap File> <Heap File Page Size> <B+ Tree Index File> <Query>");
		System.err.println("Range Query: java bpIndexQuery <Heap File> <Heap File Page Size> <B+ Tree Index File> <Query1> <Query2>\n");
		System.err.println("Equality Query (No Input heap): java bpIndexQuery " + NO_HEAP_FLAG +  " <B+ Tree Index File> <Query>");
		System.err.println("Range (No Input Heap) Query: java bpIndexQuery " + NO_HEAP_FLAG +  " <B+ Tree Index File> <Query1> <Query2>\n");
		System.err.println("Ensure queries are enclosed in quotation marks if they include spaces.");
	}
}
