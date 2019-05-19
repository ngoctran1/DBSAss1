package bpIndexLib;

public class bpNode {
	// Pointers to nodes are given as offsets in the B+ index file
	private int prevNode;
	private int nextNode;
	private int parentNode;
	private int size;
	
	// Using Alternative 2 (Store rID of records instead of record itself)
	private String[] keys;
	private int[] keyPageID;
	private int[] keySlotID;
	private int[] children;
	
	public bpNode(int prevNode, int nextNode, int parentNode, int size) {
		this.prevNode = prevNode;
		this.nextNode = nextNode;
		this.parentNode = parentNode;
		this.size = size;
		
		keys = new String[size];
		keyPageID = new int[size];
		keySlotID = new int[size];
		children = new int[size + 1];
	}

	public int getPrevNode() {
		return prevNode;
	}

	public void setPrevNode(int prevNode) {
		this.prevNode = prevNode;
	}

	public int getNextNode() {
		return nextNode;
	}

	public void setNextNode(int nextNode) {
		this.nextNode = nextNode;
	}

	public int getParentNode() {
		return parentNode;
	}

	public void setParentNode(int parentNode) {
		this.parentNode = parentNode;
	}
	
	public String getKey(int index) {
		return keys[index];
	}
	
	public int getKeyPageID(int index) {
		return keyPageID[index];
	}
	
	public int getKeySlotID(int index) {
		return keySlotID[index];
	}
	
	void setKey(int index, String key, int pageID, int slotID) {
		keys[index] = key;
		keyPageID[index] = pageID;
		keySlotID[index] = slotID;
	}
	
	public int getChild(int index) {
		return children[index];
	}
	
	public void setChild(int index, int child) {
		children[index] = child;
	}
}