package dbLoadLib;
import java.io.*;
import java.nio.*;
import java.util.ArrayList;

public class LineProcess {
	public static final int INT_SIZE = 4;
	
	// Type indicators used for calculating offsets and writing
	public static final int STR_TYPE = 0;
	public static final int INT_TYPE = 1;
	
	private static String[] data = null;
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
	
	public static ArrayList<byte[]> convertToBinary(String[] dataLine, int[] types) {
		ArrayList<byte[]> binary = new ArrayList<>(dataLine.length);
		
		for(int i = 0; i < dataLine.length; i++) {
			if(types[i] == INT_TYPE) {
				// data is in String, so convert to int then byte array
				try {
					binary.add(i, ByteBuffer.allocate(4).putInt(Integer.parseInt(dataLine[i])).array());
				} catch (NumberFormatException e) {
					binary.add(i, ByteBuffer.allocate(4).putInt(-1).array());
				}
			} else if(types[i] == STR_TYPE) {
				binary.add(i, dataLine[i].getBytes());
			}
		}
		return binary;
	}
	
	/**
	 * Used to get lines of data from input, also has ability to return data without removing it
	 * (used to calculate offsets)
	 * @param input - Input CSV file that has been opened as as Buffered Reader
	 * @paran remove - Allows caller to receive data but data is not removed (so can be retrieved again)
	 * @return - String array containing fields from input CSV file
	 */
	public static String[] getNextData(BufferedReader input, boolean remove) throws IOException {
		String[] output;

		// Return leftover data or read in next line from file
		if(data != null || hasNextData(input) == true) {
			output = data;
			if(remove) {
				data = null;
			}
		} else {
			output = null;
		}
		return output;
	}
	
	/**
	 * Used to check if there is any data left to process. Also pulls in data from file as needed.
	 * @param input - Input CSV file that has been opened as as Buffered Reader
	 * @return true if there is data remaining to be read, else false.
	 * @throws IOException
	 */
	public static boolean hasNextData(BufferedReader input) throws IOException {
		String inputLine;
		if(data == null) {
			// Read in the next data line from file
			if((inputLine = input.readLine()) == null) {
				return false;
			} else {
				data = inputLine.split(",");
				return true;
			}
		} else {
			return true;
		}
	}
}
