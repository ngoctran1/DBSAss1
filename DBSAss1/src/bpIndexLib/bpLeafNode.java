package bpIndexLib;

import java.util.Arrays;

public class bpLeafNode implements bpNode {
	private int offset;
	private bpIndexNode parent;
	private int parentOffset = -1;
	private int maxSize; // Max number of data entries
	private int size; // Current number of data entries
	private bpLeafNode nextLeaf = null;
	private bpLeafNode prevLeaf = null;
	private int nextLeafOffset = -1;
	private int prevLeafOffset = -1;
	private String[] keys;
	private int[] keyPageID;
	private int[] keySlotID;

	public bpLeafNode(int maxSize) {
		this.maxSize = maxSize;
		size = 0;
		keys = new String[maxSize];
		keyPageID = new int[maxSize];
		keySlotID = new int[maxSize];
		Arrays.fill(keyPageID, -1);
		Arrays.fill(keySlotID, -1);
	}
	
	public void setKey(int index, String key, int pageID, int slotID) {
		keys[index] = key;
		if(keyPageID[index] == -1) {
			size++;
		}
		keyPageID[index] = pageID;
		keySlotID[index] = slotID;
	}
	
	public int getKeyPageID(int index) {
		return keyPageID[index];
	}
	
	public int getKeySlotID(int index) {
		return keySlotID[index];
	}
	
	public void setKeyPageID(int index, int pageID) {
		keyPageID[index] = pageID;
	}
	
	public void setKeySlotID(int index, int slotID) {
		keySlotID[index] = slotID;
	}
	
	public String getKey(int index) {
		return keys[index];
	}
	
	public void setKey(int index, String key) {
		keys[index] = key;
	}
	
	public int addKey(String key, int pageID, int slotID) {
		if(size == maxSize) {
			System.out.println("Exceeded Size");
			return -1;
		}
		
		// Find empty spot to add entry
		for(int i = 0; i < maxSize; i++) {
			if(keyPageID[i] == -1) {
				keys[i] = key;
				keyPageID[i] = pageID;
				keySlotID[i] = slotID;
				size++;
				return i;
			}
		}
		
		return -1;
	}
	
	public bpLeafNode getNextLeaf() {
		return nextLeaf;
	}

	public void setNextLeaf(bpLeafNode nextLeaf) {
		this.nextLeaf = nextLeaf;
	}

	public bpLeafNode getPrevLeaf() {
		return prevLeaf;
	}

	public void setPrevLeaf(bpLeafNode prevLeaf) {
		this.prevLeaf = prevLeaf;
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
	
	public bpIndexNode getParent() {
		return parent;
	}
	
	public int getSize() {
		return size;
	}
	
	public void setSize(int size) {
		this.size = size;
	}
	
	public int getOffset() {
		return offset;
	}
	
	public void setOffset(int offset) {
		this.offset = offset;
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
}
