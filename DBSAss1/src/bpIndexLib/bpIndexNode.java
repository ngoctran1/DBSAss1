package bpIndexLib;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class bpIndexNode extends bpNode {
	private static final int INT_SIZE = 4;
	private static final int MAX_KEY_SIZE = 50;
	
	private ArrayList<bpNode> children;
	private ArrayList<Integer> childrenOffset;
	
	public bpIndexNode(int maxSize, int maxKeySize) {
		super(maxSize, maxKeySize);
		
		children = new ArrayList<>(maxSize + 1);
		childrenOffset = new ArrayList<>(maxSize + 1);
	}
	
	public int addKey(String key, bpNode child) {
		int index = -1;
		if(super.getSize() == super.getMaxSize()) {
			return index;
		}
		
		for(int i = 0; i < super.getSize(); i++) {
			if(key.compareTo(super.getKey(i)) < 0) {
				index = i + 1;
				super.addKey(i, key);
				children.add(index, child);
				childrenOffset.add(index, child.getOffset());
				break;
			}
		}
		
		if(index == -1) {
			children.add(child);
			childrenOffset.add(child.getOffset());
			super.addKey(key);
		}
		return index;
	}
	
	public void setKey(int index, String key) {
		if(super.getSize() <= index) {
			super.addKey(index, key);
		} else {
			super.setKey(index, key);
		}
	}
	
	public void removeKey(int index) {
		super.removeKey(index);
		children.remove(index + 1);
		childrenOffset.remove(index + 1);
	}

	public bpNode getChild(int index) {
		if(index == 0) {
			if(children.size() == 0) {
				return null;
			} else {
				return children.get(index);
			}
		} else if(index > super.getSize()) {
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
		if(index == 0 && super.getSize() == 0) {
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
	
	public void writeNode(RandomAccessFile bpFile) throws IOException {
		bpFile.writeBoolean(false);
		
		super.writeNode(bpFile);
		
		for(int i = 0; i < super.getSize(); i++) {
			bpFile.writeInt(childrenOffset.get(i));
		}
		bpFile.writeInt(childrenOffset.get(super.getSize())); // N+1 pointers
		
		// Skip empty keys
		bpFile.skipBytes(INT_SIZE * (super.getMaxSize() - super.getSize()));
	}
	
	public void readNode(byte[] data) {
		byte[] intData;
		int offset;
		
		// Read node common data
		super.readNode(data);
		
		// Only keep index node specific data
		data = Arrays.copyOfRange(data, 2 * INT_SIZE + (INT_SIZE + MAX_KEY_SIZE) * super.getMaxSize(),
										2 * INT_SIZE + (INT_SIZE + MAX_KEY_SIZE) * super.getMaxSize() + (super.getMaxSize() + 1) * INT_SIZE);
		
		// Process binary data of index node specific data
		for(int i = 0; i < super.getSize(); i++) {
			intData = Arrays.copyOfRange(data, i * INT_SIZE, (i + 1) * INT_SIZE);
			offset = ByteBuffer.wrap(intData).getInt();
			try {
				childrenOffset.set(i, offset);
			} catch (Exception e) {
				childrenOffset.add(i, offset);
			}
		}
		intData = Arrays.copyOfRange(data, super.getSize() * INT_SIZE, (super.getSize() + 1) * INT_SIZE);
		offset = ByteBuffer.wrap(intData).getInt();
		try {
			childrenOffset.set(super.getSize(), offset);
		} catch (Exception e) {
			childrenOffset.add(super.getSize(), offset);
		}
//		childrenOffset.set(super.getSize(), offset);
	}
}