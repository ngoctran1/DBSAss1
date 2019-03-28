package dbLoadLib;
import java.io.*;

public class FileProcess {
	private static final int ERROR_INT_VALUE = -1;
	
	public static void writeOffsets(int[] offsets, DataOutputStream output) throws IOException {
		for(int i = 0; i < offsets.length; i++) {
			output.writeInt(offsets[i]);
		}
	}
	
	public static void writeData(String[] data, int[] types, DataOutputStream output) throws IOException {
		for(int i = 0; i < data.length; i++) {
			if(types[i] == LineProcess.INT_TYPE) {
				try {
					output.writeInt(Integer.parseInt(data[i]));
				} catch(NumberFormatException e) {
					output.writeInt(ERROR_INT_VALUE);
				}
			} else if(types[i] == LineProcess.STR_TYPE) {
				output.write(data[i].getBytes());
			}
		}
	}
	
	public static void writeOffsets(int[] offsets, RandomAccessFile output) throws IOException {
		for(int i = 0; i < offsets.length; i++) {
			output.writeInt(offsets[i]);
		}
	}
}
