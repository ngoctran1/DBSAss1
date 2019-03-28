import java.io.*;
import java.util.Arrays;

import dbLoadLib.*;

public class dbload {
	// Used for parsing arguments into program
	private static final int EXPECTED_ARGS = 3;
	private static final int DEFAULT_ARGS = 1;
	private static final int PAGE_SIZE_INDEX = 1;
	private static final int INPUT_FILE_INDEX = 2;
	private static final int DEFAULT_PAGE_SIZE = 1024;
	
	// These constants be made to be dynamic later on if needed
	private static int maxPageSize = DEFAULT_PAGE_SIZE;
	private static boolean headerPresent = true;
	private static int numPages = 0;
	private static int[] heapDataTypes = new int[]{LineProcess.STR_TYPE, LineProcess.STR_TYPE,
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
			output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
			recordsRead = loadDB(input, output);
			output.close();
			endTime = System.nanoTime();
			input.close();
			
			System.out.println("--------------------------------------------------------------------------------");
			System.out.println("SUMMARY STATS\n");
			System.out.printf("Total Time Taken: %d ms\n", (endTime - beginTime) / 1000000);
			System.out.println("Total records read: " + recordsRead);
			System.out.println("Total pages used: " + numPages);
			System.out.println("--------------------------------------------------------------------------------");
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
	}

	
	public static int loadDB(BufferedReader input, DataOutputStream output) throws IOException {
		Page page = null;
		int recordsRead = 0;
		int[] offsets;
		Record record = null;
		String[] rawData;
		String[] processedData;
		
		while(LineProcess.hasNextData(input)) {
			// Read new data and get offsets if none remaining from previous iteration.
			rawData = LineProcess.getNextData(input, false);
			processedData = Arrays.copyOfRange(rawData, 1, rawData.length);
			processedData[0] = createDAName(rawData[0], rawData[1]);
			
			offsets = LineProcess.calcOffset(processedData, heapDataTypes);
			record = new Record(LineProcess.convertToBinary(processedData, heapDataTypes), heapDataTypes, offsets);
			
			// Create new page and add records to page
			if(page == null) {
				page = new Page(maxPageSize, numPages);
				numPages++;
			}
			
			if(page.addRecord(record)) {
				// Discard processed data
				LineProcess.getNextData(input, true);
				
				recordsRead++;
				record = null;
			} else {
				page.writePage(output);
				page = null;
			}
		}
		
		// Write last page
		if(page != null) {
			page.writePage(output);
		}
		return recordsRead;
	}
	
	/**
	 * Two formats exist in sample CSV file from Assignment:
	 * Format Type 1. mm/dd/yyyy hh:mm:ss PM/AM
	 * Format Type 2. dd/mm/yyyy hh:mm
	 * The seconds for type 2 will be set to 0 in the output
	 * @param deviceID
	 * @param arrivalTime
	 * @return DA_Name string with format deviceID-yyyy/mm/dd-hh:mm:ss where time is in 24 hour time
	 */
	private static String createDAName(String deviceID, String arrivalTime) {
		String[] dataString = arrivalTime.split(" ");
		String[] date = dataString[0].split("/");
		String[] time = dataString[1].split(":");
		
		int day;
		int month;
		int year;
		int hours;
		int minutes;
		int seconds;
		
		if(time.length == 3) {
			// Format Type 1
			month = Integer.parseInt(date[0]);
			day = Integer.parseInt(date[1]);
			year = Integer.parseInt(date[2]);
			hours = Integer.parseInt(time[0]);
			minutes = Integer.parseInt(time[1]);
			seconds = Integer.parseInt(time[2]);
			if(dataString[2].equals("PM")) {
				hours += 12;
			}
		} else {
			// Format Type 2
			day = Integer.parseInt(date[0]);
			month = Integer.parseInt(date[1]);
			year = Integer.parseInt(date[2]);
			hours = Integer.parseInt(time[0]);
			minutes = Integer.parseInt(time[1]);
			seconds = 0;
		}
		
		return String.format("%s-%s/%s/%s-%s:%s:%s",
							 deviceID,
							 year, 
							 timePadZero(month),
							 timePadZero(day),
							 timePadZero(hours),
							 timePadZero(minutes),
							 timePadZero(seconds));
	}
	
	/**
	 * Puts a 0 in front of input if input < 10 for use in formatting times / dates
	 * @param input
	 * @return
	 */
	private static String timePadZero(int input) {
		StringBuilder output = new StringBuilder();
		if(input < 10) {
			output.append(0);
		}
		output.append(input);
		return output.toString();
	}
}