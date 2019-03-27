import java.io.*;
import dbLoadLib.LineProcess;
import dbLoadLib.FileProcess;

public class dbload {
	// Performance monitoring
	private static long writeTime = 0;
	private static long checkTime = 0;
	// Used for parsing arguments into program
	private static final int EXPECTED_ARGS = 3;
	private static final int DEFAULT_ARGS = 1;
	private static final int PAGE_SIZE_INDEX = 1;
	private static final int INPUT_FILE_INDEX = 2;
	private static final int DEFAULT_PAGE_SIZE = 1024;
	
	// These constants be made to be dynamic later on if needed
	private static int maxPageSize = DEFAULT_PAGE_SIZE;
	private static int numFields = 13;
	private static boolean headerPresent = true;
	private static int headerSize = numFields * LineProcess.INT_SIZE;
	private static int[] dataTypes = new int[]{LineProcess.INT_TYPE, LineProcess.STR_TYPE, LineProcess.STR_TYPE,
													  LineProcess.INT_TYPE, LineProcess.STR_TYPE, LineProcess.STR_TYPE,
													  LineProcess.STR_TYPE, LineProcess.INT_TYPE, LineProcess.STR_TYPE,
													  LineProcess.STR_TYPE, LineProcess.STR_TYPE, LineProcess.INT_TYPE,
													  LineProcess.STR_TYPE};
	
	public static void main(String[] args) {
		String inputFile = null;
		String outputFile = null;
		DataOutputStream output = null;
		BufferedReader input;
		
		long beginTime;
		long endTime;
		int recordsRead = 0;
		int numPages = 0;
		
		// Parse arguments
		if(args.length == EXPECTED_ARGS) {
			if(args[0].equals("-p")) {
				try {
					maxPageSize = Integer.parseInt(args[PAGE_SIZE_INDEX]);
				} catch (NumberFormatException e) {
					System.err.println("Invalid page size!");
					System.exit(1);
				}
				inputFile = args[INPUT_FILE_INDEX];
				outputFile = "heap." + maxPageSize;
			} else {
				System.err.println("Invalid parameters. Please use the following:");
				System.err.println("java dbload -p pagesize datafile");
				System.exit(1);
			}
		} else if(args.length == DEFAULT_ARGS) {
			inputFile = args[0];
			outputFile = "heap." + maxPageSize;
		} else {
			System.err.println("Invalid parameters. Please use the following:");
			System.err.println("java dbload -p pagesize datafile");
			System.exit(1);
		}
		
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("INPUTS\n");
		System.out.println("Page size: " + maxPageSize + " Bytes");
		System.out.println("Input file: " + inputFile);
		System.out.println("--------------------------------------------------------------------------------");
		
		try {
			input = new BufferedReader(new FileReader(inputFile));
			
			// Remove Header
			if(headerPresent) {
				input.readLine();
			}
			
			// Process file
			beginTime = System.nanoTime();
			while(LineProcess.hasNextData(input)) {
				output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
				numPages++;
				recordsRead += loadNextPage(input, output);
			}
			output.close();
			endTime = System.nanoTime();
			input.close();
			
			System.out.println("--------------------------------------------------------------------------------");
			System.out.println("SUMMARY STATS\n");
			System.out.printf("Total Time Taken: %d ms\n", (endTime - beginTime) / 1000000);
			System.out.printf("Write Time Taken: %d ms\n", writeTime / 1000000);
			System.out.printf("Check Time Taken: %d ms\n", checkTime / 1000000);
			System.out.println("Total records read: " + recordsRead);
			System.out.println("Total pages used: " + numPages);
			System.out.println("--------------------------------------------------------------------------------");
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
	}

	
	public static int loadNextPage(BufferedReader input, DataOutputStream output) throws IOException {
		int recordsRead = 0;
		int[] offsets;
		int currentPageSize = 0;
		
		while(LineProcess.hasNextData(input)) {
			// Read new data and get offsets if none remaining from previous iteration.
			offsets = LineProcess.calcOffset(LineProcess.getNextData(input, false), dataTypes);
			
			// Close page if full
			if(currentPageSize + offsets[numFields - 1] + headerSize > maxPageSize) {
				output.close();
				break;
			}
			currentPageSize += offsets[numFields - 1] + headerSize;
			
			// Write records to heap file
			FileProcess.writeOffsets(offsets, output);
			FileProcess.writeData(LineProcess.getNextData(input, true), dataTypes, output);
			recordsRead++;
		}
		return recordsRead;
	}
}