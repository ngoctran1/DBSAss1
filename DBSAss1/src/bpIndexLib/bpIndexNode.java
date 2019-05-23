package bpIndexLib;

import java.util.ArrayList;

public class bpIndexNode implements bpNode {
	private int offset = -1;
	
	private bpIndexNode parent;
	private int parentOffset = -1;
	
	private int maxSize;
	private int size = 0;
	
	private ArrayList<String> keys;
	private ArrayList<bpNode> children;
	private ArrayList<Integer> childrenOffset;
	
	public bpIndexNode(int maxSize) {
		this.maxSize = maxSize;
		
		keys = new ArrayList<>(maxSize);
		children = new ArrayList<>(maxSize + 1);
		childrenOffset = new ArrayList<>(maxSize + 1);
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
	
	public int addKey(String key, bpNode child) {
			int insertedIndex = -1;
			if(size == maxSize) {
				return insertedIndex;
			}
			for(int i = 0; i < size; i++) {
				if(key.compareTo(keys.get(i)) < 0) {
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
	
	public void removeKey(int index) {
		keys.remove(index);
		children.remove(index + 1);
		childrenOffset.remove(index + 1);
		size--;
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