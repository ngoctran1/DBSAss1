package dbLoadLib;
import java.util.*;

public class LineProcess {
	public static final int INT_SIZE = 4;
	
	// Type indicators used for calculating offsets and writing
	public static final int STR_TYPE = 0;
	public static final int INT_TYPE = 1;
	
	/**
	 * 
	 * @param dataLine - Contains a data line in CSV file split into an array
	 * @param types - Indicates what type of value data should be treated as.
	 * @return - Array of offsets required for each field
	 */
	public static int[] calcOffset(String[] dataLine, int[] types) {
		int[] offsets = new int[dataLine.length];
		
		for(int i = 0; i < dataLine.length; i++) {
			if(types[i] == INT_TYPE) {
				// Ints are processed as 32 bit values
				if(i == 0) {
					offsets[i] = INT_SIZE;
				} else {
					offsets[i] = offsets[i - 1] + INT_SIZE;
				}
			} else if(types[i] == STR_TYPE) {
				// Strings are processed as a byte string
				offsets[i] = offsets[i - 1] + dataLine[i].getBytes().length;
			}
		}
		return offsets;
	}
	
	/**
	 * 
	 * @param input - Input CSV file that has been opened as as Buffered Reader
	 * @return - String array containing fields from input CSV file
	 */
	public static String[] getNextData(Scanner input) {
		String[] data;
		String inputLine;

		if((inputLine = input.nextLine()) != null) {
			data = inputLine.split(",");
			return data;
		} else {
			return null;
		}
	}
}
