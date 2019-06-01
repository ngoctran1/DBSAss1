package bpIndexLib;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public abstract class bpNode {
	private static final int INT_SIZE = 4;
	private static final int MAX_KEY_SIZE = 50;
	
	private int maxKeySize;
	
	private int offset = -1;
	private int maxSize; // Max number of data entries
	private int size = 0; // Current number of data entries

	private bpIndexNode parent;
	private int parentOffset = -1;
	
	private ArrayList<String> keys;
	
	public bpNode(int maxSize, int maxKeySize) {
		this.maxKeySize = maxKeySize;
		this.maxSize = maxSize;
		
		keys = new ArrayList<>();
	}
	
	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
	
	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public bpIndexNode getParent() {
		return parent;
	}

	public void setParent(bpIndexNode parent) {
		this.parent = parent;
		parentOffset = parent.getOffset();
	}

	public int getParentOffset() {
		return parentOffset;
	}

	public void setParentOffset(int parentOffset) {
		this.parentOffset = parentOffset;
	}
	
	public String getKey(int index) {
		return keys.get(index);
	}
	
	public void setKey(int index, String key) {
		if(index == keys.size()) {
			keys.add(index, key);
		} else {
			keys.set(index, key);
		}
	}
	
	public int addKey(String key) {
		if(size == maxSize) {
			return -1;
		} else {
			keys.add(key);
			size++;
			return size - 1;
		}
	}
	
	public int addKey(int index, String key) {
		if(size == maxSize) {
			return -1;
		} else {
			keys.add(index, key);
			size++;
			return index;
		}
	}
	
	public void removeKey(int index) {
		keys.remove(index);
		size--;
	}
	
	public void writeNode(RandomAccessFile bpFile) throws IOException {
		// Write out parentOffset and Size
		bpFile.writeInt(size);
		bpFile.writeInt(parentOffset);
		
		// Write out keys
		for(int i = 0; i < size; i++) {
			String key = keys.get(i);
			byte[] keyData;
			
			// Write out key length (number of bytes in string)
			keyData = key.getBytes();
			bpFile.writeInt(keyData.length);
			
			// Write out key
			bpFile.write(keyData);
			
			// Seek to next key position
			bpFile.skipBytes(maxKeySize - keyData.length);
		}
		
		// Skip empty keys
		bpFile.skipBytes((INT_SIZE + maxKeySize) * (maxSize - size));
	}
	
	public void readNode(byte[] data) {
		int keyLength;
		byte[] commonData;
		byte[] valueData;
		String key;

		// Read off parentOffset and Size
		valueData = Arrays.copyOfRange(data, 0, INT_SIZE);
		size =  ByteBuffer.wrap(valueData).getInt();
		valueData = Arrays.copyOfRange(data, INT_SIZE, 2 * INT_SIZE);
		parentOffset = ByteBuffer.wrap(valueData).getInt();
		commonData = Arrays.copyOfRange(data, 2 * INT_SIZE , 2 * INT_SIZE + (INT_SIZE + MAX_KEY_SIZE) * maxSize);
		
		// Process binary data
		for(int i = 0; i < size; i++) {
			// Read in key length
			valueData = Arrays.copyOfRange(commonData, i * (INT_SIZE + MAX_KEY_SIZE), i * (INT_SIZE + MAX_KEY_SIZE) + INT_SIZE);
			keyLength = ByteBuffer.wrap(valueData).getInt();
			
			// Read in key
			valueData = Arrays.copyOfRange(commonData, i * (INT_SIZE + MAX_KEY_SIZE) + INT_SIZE, i * (INT_SIZE + MAX_KEY_SIZE) + INT_SIZE + keyLength);
			key = new String(valueData);
			
			// Read in key binary data
			this.setKey(i, key);
		}
	}
}
