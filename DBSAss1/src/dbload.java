
import java.io.*;

public class dbload {
	private static final int DEFAULT_PAGE_SIZE = 1024;
	private static final int NUM_FIELDS = 13;
	private static final int INT_SIZE = 4;
	private static final int HEADER_SIZE = NUM_FIELDS * INT_SIZE;
	
	public static void main(String[] args) {
		int pageSize = DEFAULT_PAGE_SIZE;
		String inputFile = null;
		String outputFile = null;
		BufferedReader input;
		DataOutputStream output;
		
		// Parse arguments
		if(args.length == 3) {
			if(args[0].equals("-p")) {
				pageSize = Integer.parseInt(args[1]);
				inputFile = args[2];
				outputFile = "heap." + pageSize;
			} else {
				System.err.println("Invalid parameters. Please use the following:");
				System.err.println("java dbload -p pagesize datafile");
				System.exit(1);
			}
		} else if(args.length == 1) {
			inputFile = args[0];
			outputFile = "heap." + pageSize;
		} else {
			System.err.println("Invalid parameters. Please use the following:");
			System.err.println("java dbload -p pagesize datafile");
			System.exit(1);
		}
		
		System.out.println("Page size: " + pageSize + " Bytes");
		System.out.println("Input file: " + inputFile);
		
		try {
			output = new DataOutputStream(new FileOutputStream(outputFile));
			input = new BufferedReader(new FileReader(inputFile));
			
			String[] splitLine;
			String inputLine;
			byte[] byteString;
			int[] offsets = new int[NUM_FIELDS];
			int currentPageSize = 0;
			
			// Read in data
			input.readLine();
			while(((inputLine = input.readLine()) != null)) {
				System.out.println(currentPageSize);
				splitLine = inputLine.split(",");
				
				// Read in data and calculate offsets for record
				for(int j = 0; j < splitLine.length; j++) {
					if(j == 0) {
						offsets[j] = INT_SIZE;
					} else if(j == 3 || j == 7 || j == 11) {
						// Int values
						offsets[j] = offsets[j - 1] + INT_SIZE;
					} else {
						// String values
						byteString = splitLine[j].getBytes();
						offsets[j] = offsets[j - 1] + byteString.length;
					}
					System.out.print(offsets[j] + " ");
				}
				
				System.out.println();
				currentPageSize += offsets[NUM_FIELDS - 1] + HEADER_SIZE;
				// Reads in on extra line of data that is not written!!
				if(currentPageSize > pageSize) {
					break;
				}
				
				// Write out offsets to accomodate variable length records
				for(int j = 0; j < offsets.length; j++) {
					output.writeInt(offsets[j]);
				}
				
				// Write out data
				for(int j = 0; j < splitLine.length; j++) {	
					if(j == 0 || j == 3 || j == 7 || j == 11) {
						// Int values
						output.writeInt(Integer.parseInt(splitLine[j]));
					} else {
						// String values
						output.write(splitLine[j].getBytes());
					}
				}
			}
			output.close();
			input.close();
		} catch (IOException e) {
			System.exit(0);
		}
	}
}