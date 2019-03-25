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
				if(Double.parseDouble(data[i]) < 0) {
					output.writeInt(ERROR_INT_VALUE);
				} else {
					output.writeInt(Integer.parseInt(data[i]));
				}
			} else if(types[i] == LineProcess.STR_TYPE) {
				output.write(data[i].getBytes());
			}
		}
	}
}
