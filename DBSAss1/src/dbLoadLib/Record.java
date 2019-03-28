package dbLoadLib;
import java.nio.ByteBuffer;
import java.util.*;

public class Record {
	public final int INT_SIZE = Integer.SIZE/8;
	private int[] dataTypes;
	private int numFields;
	private int[] offsets;
	private ArrayList<byte[]> data;
	
	public Record(ArrayList<byte[]> data, int[] dataTypes, int[] offsets) {
		this.data = data;
		this.dataTypes = dataTypes;
		this.offsets = offsets;
		numFields = dataTypes.length;
	}
	
	// Returns offsets of records with the beginning of the record at "shift" offset (instead of 0)
	public int[] getOffsets(int shift) {
		int[] result = Arrays.copyOfRange(offsets, 0, numFields);
		if(shift != 0) {
			for(int i = 0; i < result.length; i++) {
				result[i] += shift;
			}
		}
		return result;
	}
	
	public int[] getDataTypes() {
		return dataTypes;
	}

	public ArrayList<byte[]> getData() {
		return data;
	}
	
	// Gets total number of bytes to be written for offsets + data
	public int getTotalRecordSize() {
		int result = 0;
		for(byte[] attribute : data) {
			result += attribute.length;
		}
		return result + offsets.length * INT_SIZE;
	}
	
	public String getReadableData() {
		StringBuilder output = new StringBuilder();
		ByteBuffer buffer;
		
		for(int i = 0; i < numFields; i++) {
			if(dataTypes[i] == LineProcess.INT_TYPE) {
				buffer = ByteBuffer.wrap(data.get(i));
				output.append(buffer.getInt());
			} else if(dataTypes[i] == LineProcess.STR_TYPE) {
				output.append(new String(data.get(i)));
			}
			output.append(" | ");
		}
		
		return output.toString();
	}
}
