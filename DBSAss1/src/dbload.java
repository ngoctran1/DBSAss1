
import java.io.*;
import java.util.*;
import dbLoadLib.LineProcess;
import dbLoadLib.FileProcess;

public class dbload {
	// Used for parsing arguments into program
	private static final int EXPECTED_ARGS = 3;
	private static final int DEFAULT_ARGS = 1;
	private static final int PAGE_SIZE_INDEX = 1;
	private static final int INPUT_FILE_INDEX = 2;
	private static final int DEFAULT_PAGE_SIZE = 1024;
	
	// These constants be made to be dynamic later on if needed
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
		DataOutputStream output;
		Scanner input;
		
		int currentPageSize = 0;
		int recordsRead = 0;
		int numPages = 0;
		int pageSize = DEFAULT_PAGE_SIZE;
		
		String[] data = null;
		int[] offsets = new int[numFields];
		boolean prevData = false; // Used to indicate extra data from full page needs to be written to next page
		
		// Parse arguments
		if(args.length == EXPECTED_ARGS) {
			if(args[0].equals("-p")) {
				try {
					pageSize = Integer.parseInt(args[PAGE_SIZE_INDEX]);
				} catch (NumberFormatException e) {
					System.err.println("Invalid page size!");
					System.exit(1);
				}
				inputFile = args[INPUT_FILE_INDEX];
				outputFile = "heap." + pageSize;
			} else {
				System.err.println("Invalid parameters. Please use the following:");
				System.err.println("java dbload -p pagesize datafile");
				System.exit(1);
			}
		} else if(args.length == DEFAULT_ARGS) {
			inputFile = args[0];
			outputFile = "heap." + pageSize;
		} else {
			System.err.println("Invalid parameters. Please use the following:");
			System.err.println("java dbload -p pagesize datafile");
			System.exit(1);
		}
		
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("INPUTS\n");
		System.out.println("Page size: " + pageSize + " Bytes");
		System.out.println("Input file: " + inputFile);
		System.out.println("--------------------------------------------------------------------------------");
		
		try {
			input = new Scanner(new File(inputFile));
			output = new DataOutputStream(new FileOutputStream(outputFile));
			numPages++;
			
			// Remove Header
			if(headerPresent) {
				input.nextLine();
			}
			
			// Process file
			while(true) {
				while(currentPageSize < pageSize && (input.hasNextLine() || prevData == true)) {
					// Read new data and get offsets if none remaining from previous iteration.
					if(prevData == false) {
						data = LineProcess.getNextData(input);
						offsets = LineProcess.calcOffset(data, dataTypes);
					}
					
					// Determine if there is enough free space to input record
					if(currentPageSize + offsets[numFields - 1] + headerSize > pageSize) {
						prevData = true;
						output.close();
						break;
					}
					currentPageSize += offsets[numFields - 1] + headerSize;
					
					// Write records to heap file
					FileProcess.writeOffsets(offsets, output);
					FileProcess.writeData(data, dataTypes, output);
					
					prevData = false;
					recordsRead++;
				}
				
				// Only create a new page file if there is data left to be written
				if(input.hasNextLine() || prevData == true) {
					output = new DataOutputStream(new FileOutputStream(outputFile));
					numPages++;
					currentPageSize = 0;
				} else {
					break;
				}
			}
			
			output.close();
			input.close();
			
			System.out.println("--------------------------------------------------------------------------------");
			System.out.println("SUMMARY STATS\n");
			System.out.println("Total records read: " + recordsRead);
			System.out.println("Total pages used: " + numPages);
			System.out.println("--------------------------------------------------------------------------------");
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
	}
}