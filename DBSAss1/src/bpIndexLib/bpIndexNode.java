package bpIndexLib;

import java.util.ArrayList;
import java.util.Arrays;

public class bpIndexNode extends bpNode {
	// Pointers to nodes are given as offsets in the B+ index file
	private bpIndexNode parentNode;
	private int maxSize;
	private int size = 0;
	
	// Using Alternative 2 (Store rID of records instead of record itself)
	private ArrayList<String> keys;
	private ArrayList<bpNode> children;
	
	public bpIndexNode(int maxSize) {
		this.maxSize = maxSize;
		
		keys = new ArrayList(maxSize);
		children = new ArrayList(maxSize + 1);
//		for(int i = 0; i < maxSize + 1; i++) {
//			children.set(i, -1);
//		}
	}
	
	public bpIndexNode getParentNode() {
		return parentNode;
	}

	public void setParentNode(bpIndexNode parentNode) {
		this.parentNode = parentNode;
	}
	
	public String getKey(int index) {
		return keys.get(index);
	}
	
	void setKey(int index, String key) {
		keys.set(index, key);
	}
	
	public bpNode getChild(int index) {
		return children.get(index);
	}
	
	public void setChild(int index, bpNode child) {
		if(index == 0 && size == 0) {
			children.add(child);
		} else {
			children.set(index, child);
		}
	}
	
	public int addKey(String key, bpNode child) {
		int insertedIndex = -1;
		if(size == maxSize) {
			return insertedIndex;
		}
		for(int i = 0; i < size; i++) {
			if(key.compareTo(keys.get(i)) < 0) {
				if(i == 0) {
					// TODO: Check whether the node goes after or before P0
					insertedIndex = i + 1;
				} else {
					insertedIndex = i + 1;
				}
				children.add(insertedIndex, child);
				keys.add(i, key);
				size++;
				break;
			}
		}
		if(insertedIndex == -1) {
			children.add(child);
			keys.add(key);
			size++;
		}
		return insertedIndex;
	}
	
	public int getSize() {
		return size;
	}
	
	public void removeKey(int index) {
		keys.remove(index);
		children.remove(index + 1);
		size--;
	}
}