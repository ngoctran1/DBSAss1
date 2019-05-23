package bpIndexLib;

import java.util.ArrayList;
import java.util.Arrays;

public class bpIndexNode implements bpNode {
	// Pointers to nodes are given as offsets in the B+ index file?
	private bpIndexNode parent;
	private int parentOffset = -1;
	private int maxSize;
	private int size = 0;
	private int offset = -1;
	
	// Using Alternative 2 (Store rID of records instead of record itself)
	private ArrayList<String> keys;
	private ArrayList<bpNode> children;
	private ArrayList<Integer> childrenOffset;
	
	public bpIndexNode(int maxSize) {
		this.maxSize = maxSize;
		
		keys = new ArrayList(maxSize);
		children = new ArrayList(maxSize + 1);
		childrenOffset = new ArrayList(maxSize + 1);
	}
	
	public bpIndexNode getParent() {
		// if parent is not already in memory, retrieve from database
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
		if(keys.size() <= index) {
			keys.add(index, key);
		} else {
			keys.set(index, key);
		}
	}
	
	public bpNode getChild(int index) {
		if(index == 0) {
			if(children.size() == 0) {
				return null;
			} else {
				return children.get(index);
			}
		} else if(index > size) {
			return null;
		} else {
			if(children.size() <= index) {
				return null;
			} else {
				return children.get(index);
			}
		}
		
	}
	
	public void setChild(int index, bpNode child) {
		if(index == 0 && size == 0) {
			children.add(child);
			childrenOffset.add(child.getOffset());
		} else {
			childrenOffset.set(index, child.getOffset());
		}
	}
	
	public int addKey(String key, bpNode child) {
		int insertedIndex = -1;
		if(size == maxSize) {
			return insertedIndex;
		}
		for(int i = 0; i < size; i++) {
			if(key.compareTo(keys.get(i)) < 0) {
//				if(i == 0) {
//					// TODO: Check whether the node goes after or before P0
//					insertedIndex = i + 1;
//				} else {
//					
//				}
				insertedIndex = i + 1;
				children.add(insertedIndex, child);
				childrenOffset.add(insertedIndex, child.getOffset());
				keys.add(i, key);
				size++;
				break;
			}
		}
		if(insertedIndex == -1) {
			children.add(child);
			childrenOffset.add(child.getOffset());
			keys.add(key);
			size++;
		}
		return insertedIndex;
	}
	
	public int getSize() {
		return size;
	}
	
	public void setSize(int size) {
		this.size = size;
	}
	
	public void removeKey(int index) {
		keys.remove(index);
		children.remove(index + 1);
		childrenOffset.remove(index + 1);
		size--;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	public int getChildOffset(int index) {
		return childrenOffset.get(index);
	}
	
	public void setChildOffset(int index, int offset) {
		if(childrenOffset.size() <= index) {
			childrenOffset.add(index, offset);
		} else {
			childrenOffset.set(index, offset);
		}
	}
}