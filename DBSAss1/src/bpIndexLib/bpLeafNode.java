package bpIndexLib;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class bpLeafNode extends bpNode {
	private static final int INT_SIZE = 4;
	private static final int MAX_KEY_SIZE = 50;

	private bpLeafNode nextLeaf = null;
	private bpLeafNode prevLeaf = null;
	private int nextLeafOffset = -1;
	private int prevLeafOffset = -1;

	private int[] keyPageID;
	private int[] keySlotID;

	public bpLeafNode(int maxSize, int maxKeySize) {
		super(maxSize, maxKeySize);
		
		keyPageID = new int[maxSize];
		keySlotID = new int[maxSize];
		Arrays.fill(keyPageID, -1);
		Arrays.fill(keySlotID, -1);
	}
	
	public bpLeafNode(int maxSize, int maxKeySize, byte[] data) {
		super(maxSize, maxKeySize);
		
		keyPageID = new int[maxSize];
		keySlotID = new int[maxSize];
		Arrays.fill(keyPageID, -1);
		Arrays.fill(keySlotID, -1);
	}

	public bpLeafNode getNextLeaf() {
		return nextLeaf;
	}

	public void setNextLeaf(bpLeafNode nextLeaf) {
		this.nextLeaf = nextLeaf;
		nextLeafOffset = nextLeaf.getOffset();
	}

	public bpLeafNode getPrevLeaf() {
		return prevLeaf;
	}

	public void setPrevLeaf(bpLeafNode prevLeaf) {
		this.prevLeaf = prevLeaf;
		prevLeafOffset = prevLeaf.getOffset();
	}

	public int getNextLeafOffset() {
		return nextLeafOffset;
	}

	public void setNextLeafOffset(int nextLeafOffset) {
		this.nextLeafOffset = nextLeafOffset;
	}

	public int getPrevLeafOffset() {
		return prevLeafOffset;
	}

	public void setPrevLeafOffset(int prevLeafOffset) {
		this.prevLeafOffset = prevLeafOffset;
	}

	public int addKey(String key, int pageID, int slotID) {
		int index = super.addKey(key);
		keyPageID[index] = pageID;
		keySlotID[index] = slotID;
		return index;
	}

	public int getKeyPageID(int index) {
		return keyPageID[index];
	}
	
	public void setKeyPageID(int index, int pageID) {
		keyPageID[index] = pageID;
	}

	public int getKeySlotID(int index) {
		return keySlotID[index];
	}
	
	public void setKeySlotID(int index, int slotID) {
		keySlotID[index] = slotID;
	}
	
	public void writeNode(RandomAccessFile bpFile) throws IOException {
		// Write leaf node flag
		bpFile.writeBoolean(true);
		
		super.writeNode(bpFile);
		
		for(int i = 0; i < super.getSize(); i++) {
			bpFile.writeInt(keyPageID[i]);
			bpFile.writeInt(keySlotID[i]);
		}
		// Skip empty keys
		bpFile.skipBytes(2 * INT_SIZE * (super.getMaxSize() - super.getSize()));
		
		bpFile.writeInt(nextLeafOffset);
		bpFile.writeInt(prevLeafOffset);
	}
	
	public void readNode(byte[] data) {
		byte[] intData;
		
		// Read node common data
		super.readNode(data);
		
		// Only keep leaf node specific data
		data = Arrays.copyOfRange(data, 2 * INT_SIZE + (INT_SIZE + MAX_KEY_SIZE) * super.getMaxSize(),
										2 * INT_SIZE + (INT_SIZE + MAX_KEY_SIZE) * super.getMaxSize() + (2 * super.getMaxSize() + 2) * INT_SIZE);
		
		// Process binary data of leaf node specific data
		for(int i = 0; i < super.getSize(); i++) {
			intData = Arrays.copyOfRange(data, 2 * i * INT_SIZE, (2 * i + 1) * INT_SIZE);
			keyPageID[i] =  ByteBuffer.wrap(intData).getInt();
			
			intData = Arrays.copyOfRange(data, (2 * i + 1) * INT_SIZE, (2 * i + 2) * INT_SIZE);
			keySlotID[i] =  ByteBuffer.wrap(intData).getInt();
		}
		intData = Arrays.copyOfRange(data, 2 * super.getSize() * INT_SIZE, (2 * super.getSize() + 1) * INT_SIZE);
		nextLeafOffset = ByteBuffer.wrap(intData).getInt();
		
		intData = Arrays.copyOfRange(data, (2 * super.getSize() + 1) * INT_SIZE, (2 * super.getSize() + 2) * INT_SIZE);
		prevLeafOffset = ByteBuffer.wrap(intData).getInt();
	}
}
