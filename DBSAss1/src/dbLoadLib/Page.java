package dbLoadLib;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class Page {
	public final int INT_SIZE = Integer.SIZE/8;
	
	private final int RECORD_OFFSET_INDEX = 0;
	private final int RECORD_SIZE_INDEX = 1;
	
	private int pageSize;
	private int dataSize;
	private int footerSize;
	private int pageID;
	private int numSlots;
	private int freeSpace;
	private ArrayList<Record> slots;
	
	private int forwardCursor;
	private int backwardCursor;
	
	// <offset, size>
	private ArrayList<int[]> slotDirectory;
	
	private ArrayList<Integer> slotID;
	private int freeSpaceOffset;
	
	public Page(int pageSize, int pageID) {
		this.pageSize = pageSize;
		this.pageID = pageID;
		
		freeSpace = pageSize - 3 * INT_SIZE; // pageSize - pageID - freeSpaceOffset - numSlots
		dataSize = 0;
		freeSpaceOffset = 0;
		numSlots = 0;
		slots = new ArrayList<>(100);
		slotID = new ArrayList<>(100);
		slotDirectory = new ArrayList<>(100);
		
		// Footer initially stores numSlots (0) and pageID
		footerSize = 2 * INT_SIZE;
	}
	
	// Used to reconstruct a page from binary page data
	public Page(byte[] inputData, int[] dataTypes) {
		int slotOffset;
		int slotSize;
		slots = new ArrayList<>(100);
		slotID = new ArrayList<>(100);
		slotDirectory = new ArrayList<>(100);
		int[] offsets;
		ArrayList<byte[]> data;
		Record record;
		
		pageSize = inputData.length;
		forwardCursor = 0;
		backwardCursor = inputData.length;
		
		pageID = binToInt(readDataForward(inputData, INT_SIZE));
		
		freeSpaceOffset = binToInt(readDataBackward(inputData, INT_SIZE));
		
		// Reconstruct slotDirectory
		numSlots = binToInt(readDataBackward(inputData, INT_SIZE));
		for(int i = 0; i < numSlots; i++) {
			slotSize =  binToInt(readDataBackward(inputData, INT_SIZE));
			slotOffset = binToInt(readDataBackward(inputData, INT_SIZE));
			slotDirectory.add(new int[]{slotOffset, slotSize});
			
			// Read corresponding slot
			slotID.add(binToInt(readDataForward(inputData, INT_SIZE)));
			
			// Read record data offsets
			offsets = new int[dataTypes.length];
			for(int j = 0; j < dataTypes.length; j++) {
				offsets[j] = binToInt(readDataForward(inputData, INT_SIZE));
			}
			
			// Read record data
			data = new ArrayList<>();
			for(int j = 0; j < dataTypes.length; j++) {
				if(j == 0) {
					data.add(readDataForward(inputData, offsets[j]));
				} else {
					data.add(readDataForward(inputData, offsets[j] - offsets[j - 1]));
				}
				
			}
			record = new Record(data, dataTypes, offsets);
			slots.add(record);
		}
	}
	
	public int getPageID() {
		return pageID;
	}
	
	public boolean addRecord(Record record) {
		int[] freeGap = findEmptySlot(record.getTotalRecordSize());
		if(freeGap != null) {
			// Add record into existing slot
			slots.add(freeGap[0], record);
			slotDirectory.add(freeGap[0], new int[] {freeGap[1], record.getTotalRecordSize()});
			return true;
		} else if(freeSpace > record.getTotalRecordSize() + 3*INT_SIZE){
			// Add record into a new slot
			slots.add(numSlots, record);
			slotDirectory.add(numSlots, new int[] {freeSpaceOffset, record.getTotalRecordSize()});
			slotID.add(numSlots, numSlots);
			footerSize += 2*INT_SIZE;
			numSlots++;
			freeSpaceOffset += record.getTotalRecordSize() + INT_SIZE;
			freeSpace -= record.getTotalRecordSize() + 3*INT_SIZE;
			return true;
		} else {
			return false;
		}
	}
	
	public int getFreeSpace() {
		return freeSpace;
	}

	// Returns the pair <Slot index, slot offset> for big enough empty slot
	private int[] findEmptySlot(int recordSize) {
		int startGap = -1;
		int startGapSlot = -1;
		int endGap;
		
		for(int i = 0; i < slotDirectory.size(); i++) {
			// Find beginning of gap in slots
			if(slotDirectory.get(i)[RECORD_OFFSET_INDEX] == -1) {
				if(i == 0) {
					// First slot is empty
					startGap = 0;
					startGapSlot = 0;
				} else if(startGap == -1) {
					startGap = slotDirectory.get(i - 1)[RECORD_OFFSET_INDEX] + slotDirectory.get(i - 1)[RECORD_SIZE_INDEX];
					startGapSlot = i;
				}
			} else if(startGap >= 0) {
				endGap = slotDirectory.get(i)[RECORD_OFFSET_INDEX];
				
				// Check if gap is large enough
				if(endGap - startGap > recordSize) {
					return new int[] {startGapSlot, startGap};
				} else {
					startGap = -1;
				}
			} else {
				startGap = -1;
			}
		}
		return null;
	}
	
	public void writePage(DataOutputStream output) throws IOException {
		Record record;
		int[] offsets;
		ArrayList<byte[]> data;
		int[] slot;
		
		output.writeInt(pageID);
		
		// Write Records
		for(int i = 0; i < slots.size(); i++) {
			record = slots.get(i);
			offsets = record.getOffsets(0);
			data = record.getData();
			
			// Write slotIDs and offsets
			output.writeInt(slotID.get(i));
			for(int j = 0; j < offsets.length; j++) {
				output.writeInt(offsets[j]);
			}
			
			// Write data
			for(int j = 0; j < data.size(); j++) {
				output.write(data.get(j));
			}
		}
		
		// Skip free space (write 0)
		for(int i = 0; i < freeSpace; i++) {
			output.write(0);
		}
		
		// Write slot directory
		for(int i = slotDirectory.size(); i > 0; i--) {
			slot = slotDirectory.get(i - 1);
			output.writeInt(slot[0]);
			output.writeInt(slot[1]);
		}
		
		output.writeInt(numSlots);
		output.writeInt(freeSpaceOffset);
	}
	
	// Converts binary to integer
	private int binToInt(byte[] input) {
		ByteBuffer buffer = ByteBuffer.wrap(input);
		return buffer.getInt();
	}
	
	public Record getSlotByIndex(int slotIndex) {
		return slots.get(slotIndex);
	}
	
	public int getNumRecords() {
		return slots.size();
	}
	
	public int getSlotID(int index) {
		return slotID.get(index);
	}
	
	private byte[] readDataForward(byte[] inputData, int byteSize) {
		if(forwardCursor + byteSize > pageSize) {
			return null;
		}
		byte[] result = Arrays.copyOfRange(inputData, forwardCursor, forwardCursor + byteSize);
		forwardCursor += byteSize;
		return result;
	}
	
	private byte[] readDataBackward(byte[] inputData, int byteSize) {
		if(backwardCursor - byteSize < 0) {
			return null;
		}
		byte[] result = Arrays.copyOfRange(inputData, backwardCursor - byteSize, backwardCursor);
		backwardCursor -= byteSize;
		return result;
	}
}
