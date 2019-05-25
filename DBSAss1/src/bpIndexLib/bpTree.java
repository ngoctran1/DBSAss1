package bpIndexLib;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

import dbLoadLib.Heap;
import dbLoadLib.Page;

public class bpTree {
	private static final int INT_SIZE = 4;
	private static final int MAX_FIELD_SIZE = 50;

	// Leaf nodes are bigger than index nodes. They contain (2N+6) INT and N STR where N number of keys
	private int nodeByteSize; 
	private bpIndexNode root;
	private bpLeafNode firstLeaf;
	private bpLeafNode lastLeaf;
	private int nodeMaxSize;
	private RandomAccessFile bpFile;
	private String bpFileName;
	
	private long heapFileReadTime;
	
	private int numLeavesRead = 0;
	
	// Read pre-existing B+ tree
	public bpTree(RandomAccessFile bpFile) {
		this.bpFile = bpFile;
		
		System.err.println("READING B+ INDEX FILE\n");
		try {
			nodeMaxSize = bpFile.readInt();
			nodeByteSize = (3 * nodeMaxSize + 6) * INT_SIZE + nodeMaxSize * (MAX_FIELD_SIZE + 1) + 2;
			
			System.err.println("nodeByteSize = " + nodeByteSize);
			System.err.println("Max Node Size: " + nodeMaxSize);
			
			// Set root offset
			root = new bpIndexNode(nodeMaxSize);
			root.setOffset(bpFile.readInt());
			
			// Read in first leaf node
			firstLeaf = new bpLeafNode(nodeMaxSize);
			firstLeaf.setOffset((int)bpFile.getFilePointer());
			bpFile.readBoolean();
			readLeafNode(firstLeaf);
			
			// Read in root node
			bpFile.seek(root.getOffset());
			bpFile.readBoolean();
			readIndexNode(root);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	// Bulk Load
	public bpTree(BufferedReader inputData, int nodeMaxSize) {
		nodeByteSize = (3 * nodeMaxSize + 6) * INT_SIZE + nodeMaxSize * (MAX_FIELD_SIZE + 1) + 2;
		this.nodeMaxSize = nodeMaxSize;
		root = new bpIndexNode(nodeMaxSize);
		String[] dataLine = null;
		boolean setFirstPointer = false;
		int numLeafNodes = 0;
		bpLeafNode newLeafNode = null;
		
		// Create new index file
		try {
			bpFileName = "bpIndex." + nodeMaxSize;
			bpFile = new RandomAccessFile(bpFileName, "rw");
			bpFile.setLength(2 * INT_SIZE);
			
			// Header of file
			bpFile.writeInt(nodeMaxSize);
			bpFile.writeInt(root.getOffset());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		outer:while(true) {	
			// Create Leaf Node and read in data
			newLeafNode = new bpLeafNode(nodeMaxSize);
			for(int i = 0; i < nodeMaxSize; i++) {
				try {
					dataLine = inputData.readLine().split(",");
				} catch (Exception e) {
					break outer;
				}
				newLeafNode.addKey(dataLine[0], Integer.parseInt(dataLine[1]), Integer.parseInt(dataLine[2]));
			}
			writeNode(newLeafNode);
			
			// Add leaf node to tree
			if(!setFirstPointer) {
				insertFirstNode(newLeafNode);
				
				firstLeaf = newLeafNode;
				lastLeaf = newLeafNode;
				setFirstPointer = true;
				
				writeNode(newLeafNode);
			} else {
				insertLeafNode(newLeafNode);
				
				// Update leaf node pointers
				lastLeaf.setNextLeaf(newLeafNode);
				writeNode(lastLeaf);
				newLeafNode.setPrevLeaf(lastLeaf);
				writeNode(newLeafNode);
				
				lastLeaf = newLeafNode;
			}
			newLeafNode = null;
			numLeafNodes++;
		}
		
		// Add final node if it isn't empty
		if(newLeafNode != null && newLeafNode.getSize() > 0) {
			writeNode(newLeafNode);
			if(!setFirstPointer) {
				insertFirstNode(newLeafNode);
				firstLeaf = newLeafNode;
				lastLeaf = newLeafNode;
				
				writeNode(newLeafNode);
			} else {
				insertLeafNode(newLeafNode);
				
				// Update leaf nodes
				lastLeaf.setNextLeaf(newLeafNode);
				writeNode(lastLeaf);
				newLeafNode.setPrevLeaf(lastLeaf);
				writeNode(newLeafNode);
				
				lastLeaf = newLeafNode;
			}
		}
		
		System.err.println("B+ Tree Created");
		System.err.println("Num Leaf Nodes = " + numLeafNodes);
		System.err.println("Index File Generated: " + bpFileName);
	}
	
	private void insertFirstNode(bpLeafNode node) {
		root.setChild(0, node);
		updateRoot(root);
		node.setParent(root);
	}
	
	private void insertLeafNode(bpLeafNode node) {
		bpIndexNode currentNode = root;
		bpIndexNode destNode = null;
		
		destNode = traverseBottomRight(currentNode, node.getKey(0));
		
		// Push key value into this node
		pushUp(destNode, node, node.getKey(0));
		
		writeNode(destNode);
	}
	
	// Adds in newKeyNode as a child of pushIntoNode and sets the key for it as newKey
	// Splits node if already full and pushes middle key up into parent node
	private void pushUp(bpIndexNode pushIntoNode, bpNode newKeyNode, String newKey) {
		if(pushIntoNode.getSize() == nodeMaxSize) {
			bpIndexNode newNode = new bpIndexNode(nodeMaxSize);
			
			int startCopyIndex = ceilingDivision(pushIntoNode.getSize(), 2) + 1;
			int copyLength = (pushIntoNode.getSize() / 2) - 1; // Second half - Key to push up
			int pushValueIndex = startCopyIndex - 1;
			
			// Preserve pointer of the value about to be pushed by storing it in the first pointer in new node
			String pushKey = pushIntoNode.getKey(pushValueIndex);
			
			// Copy over data from full node into new node (split)
			newNode.setChild(0, readChildNode(pushIntoNode, startCopyIndex));
			for(int i = 0; i < copyLength; i++) {
				newNode.addKey(pushIntoNode.getKey(startCopyIndex + i), readChildNode(pushIntoNode, startCopyIndex + i + 1));
			}
			newNode.addKey(newKey, newKeyNode);
			
			// Delete copied over data
			for(int i = 0; i < copyLength + 1; i++) {
				pushIntoNode.removeKey(pushValueIndex);
			}
			
			writeNode(newNode);
			newKeyNode.setParent(newNode);
			
			// Push middle value up tree
			if(readParentNode(pushIntoNode) == null) {
				// Needs new root
				bpIndexNode newRoot = new bpIndexNode(nodeMaxSize);
				newRoot.setChild(0, pushIntoNode);
				newRoot.setChildOffset(0, pushIntoNode.getOffset());
				updateRoot(newRoot);
				
				pushIntoNode.setParent(newRoot);
				pushIntoNode.setParentOffset(newRoot.getOffset());
				root = newRoot;
				
				writeNode(pushIntoNode);
				writeNode(newKeyNode);
				
				pushUp(newRoot, newNode, pushKey);
			} else {
				writeNode(pushIntoNode);
				writeNode(newKeyNode);
				
				pushUp(readParentNode(pushIntoNode), newNode, pushKey);
			}
		} else {
			// Node has free space, just add in
			pushIntoNode.addKey(newKey, newKeyNode);
			newKeyNode.setParent(pushIntoNode);
			
			writeNode(pushIntoNode);
			writeNode(newKeyNode);
		}
	}
	
	// Read DB flag indicates whether to pull entire record from heap file (significantly slows performance!)
	public void equalityQuery(String query, boolean findAll, boolean readDB, Heap heap, BufferedWriter output) {
		System.err.println("Equality Query");
		heapFileReadTime = 0;
		
		bpIndexNode currentNode = root;
		bpLeafNode result = traverseBottomLeft(currentNode, query);
		boolean matched = false;
		
		// Find a matching key in the startNode
		for(int i = 0; i < result.getSize(); i++) {
			if(result.getKey(i).compareTo(query) == 0) {
				// Write out result
				try {
					if(readDB) {
						long startTime = System.nanoTime();
						Page page = heap.getPage(result.getKeyPageID(i));
						heapFileReadTime += System.nanoTime() - startTime;
						
						output.write(page.getSlotByIndex(result.getKeySlotID(i)).getReadableData());
					} else {
						StringBuilder outputString = new StringBuilder();
						outputString.append(result.getKey(i));
						outputString.append(", PageID: ");
						outputString.append(result.getKeyPageID(i));
						outputString.append(", SlotID: ");
						outputString.append(result.getKeySlotID(i));
						output.write(outputString.toString());
					}
					output.write("\n");
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
				
				matched = true;
				
				// Stop if only a single match is needed
				if(!findAll) {
					return;
				}
			} else {
				if(matched == true) {
					// No more matches to be found due to sorted linked list
					return;
				}
			}
		}
		
		// Find matches in next leaf node if exists
		findRemainMatches(readNextLeaf(result), query, readDB, heap, output);
	}
	
	public int rangeQuery(String lowBound, String highBound, boolean readDB, Heap heap, BufferedWriter output) {
		boolean matched = false;
		boolean brokenChain = false;
		int numMatches = 0;
		int numMismatch;
		
		heapFileReadTime = 0;
		
		// Invalid inputs
		if(lowBound.compareTo(highBound) > 0) {
			return numMatches;
		}
		
		bpIndexNode currentNode = root;
		
		bpLeafNode result = traverseBottomLeft(currentNode, lowBound);
		
		// Continue to read leaf nodes until final leaf node or no more matches
		while(result != null) {
			numMismatch = 0;
			
			for(int i = 0; i < result.getSize(); i++) {
				String key = result.getKey(i);
				
				// Write out result if matches query, else break if no more matches to be found
				if(key.compareTo(lowBound) >= 0 && key.compareTo(highBound) < 0) {
					try {						
						if(readDB) {
							long startTime = System.nanoTime();
							Page page = heap.getPage(result.getKeyPageID(i));
							heapFileReadTime += System.nanoTime() - startTime;
							
							output.write(page.getSlotByIndex(result.getKeySlotID(i)).getReadableData());
						} else {
							StringBuilder outputString = new StringBuilder();
							outputString.append(result.getKey(i));
							outputString.append(", PageID: ");
							outputString.append(result.getKeyPageID(i));
							outputString.append(", SlotID: ");
							outputString.append(result.getKeySlotID(i));
							output.write(outputString.toString());
						}
						output.write("\n");
					} catch (IOException e) {
						e.printStackTrace();
						System.exit(1);
					}
					matched = true;
					numMatches++;
				} else if (matched == true) {
					// Previously found matches, then found where matches stop
					// Since leaf nodes are sorted, no need to check anymore nodes
					brokenChain = true;
					break;
				} else {
					numMismatch++;
				}
			}

			if(brokenChain == true || numMismatch == result.getSize()) {
				break;
			} else {
				result = readNextLeaf(result);
			}
		}
		
		return numMatches;
	}
	
	// Keeps scanning successive leaf nodes until it comes across a key not satisfying the query
	private void findRemainMatches(bpLeafNode startNode, String query, boolean readDB, Heap heap, BufferedWriter output) {
		if(startNode == null) {
			return;
		}
		
		for(int i = 0; i < startNode.getSize(); i++) {
			if(startNode.getKey(i).compareTo(query) == 0) {
				// Match, so write out results
				try {
					if(readDB) {
						long startTime = System.nanoTime();
						Page page = heap.getPage(startNode.getKeyPageID(i));
						heapFileReadTime += System.nanoTime() - startTime;
						
						output.write(page.getSlotByIndex(startNode.getKeySlotID(i)).getReadableData());
					} else {
						StringBuilder outputString = new StringBuilder();
						outputString.append(startNode.getKey(i));
						outputString.append(", PageID: ");
						outputString.append(startNode.getKeyPageID(i));
						outputString.append(", SlotID: ");
						outputString.append(startNode.getKeySlotID(i));
						output.write(outputString.toString());
					}
					output.write("\n");
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
				
			} else {
				return;
			}
		}
		
		findRemainMatches(readNextLeaf(startNode), query, readDB, heap, output);
	}

	// Find the BOTTOM-RIGHT most index node into which a leaf node should be inserted
	private bpIndexNode traverseBottomRight(bpIndexNode startNode, String key) {
		// If next nodes are leaf nodes return this node
		if(readChildNode(startNode, 0) instanceof bpLeafNode || readChildNode(startNode, 0) == null) {
			return startNode;
		}
		
		// Look through values to see which subtree should be traversed
		for(int i = 0; i < startNode.getSize(); i++) {
			// Find first key that is larger than search key
			if(key.compareTo(startNode.getKey(i)) < 0) {
				return traverseBottomRight((bpIndexNode) readChildNode(startNode, i), key);
			}
		}
		
		// Search key is larger or equal to largest key value, so traverse final child
		return traverseBottomRight((bpIndexNode) readChildNode(startNode, startNode.getSize()), key);
	}

	// Uses query to traverse down tree to BOTTOM-LEFT most leaf node that contains the query.
	private bpLeafNode traverseBottomLeft(bpIndexNode start, String query) {
		for(int i = 0; i < start.getSize(); i++) {
			if(start.getKey(i).compareTo(query) >= 0) { // Includes equality due to duplicates
				if(readChildNode(start, i) instanceof bpIndexNode) {
					// Checks to see if subtree before matched key contains query (possible due to duplicates)
					bpLeafNode potentialResult = traverseBottomLeft((bpIndexNode)readChildNode(start, i), query);
					
					if(potentialResult != null) {
						// Go into node before key
						return potentialResult;
					} else {
						// Go into node after key
						return traverseBottomLeft((bpIndexNode) readChildNode(start, i + 1), query);
					}
				} else {
					bpLeafNode potentialResult = (bpLeafNode)readChildNode(start, i);
					
					if(scanLeaf(potentialResult, query)) {
						// Checks if leaf node just before matched key contains query (possible due to duplicates)
						return potentialResult;
					} else {
						// Return leaf node after matched key
						return readNextLeaf(potentialResult);
					}
				}
			}
		}
		
		// No keys match, so traverse the last pointer
		if(readChildNode(start, start.getSize()) instanceof bpIndexNode) {
			return traverseBottomLeft((bpIndexNode)readChildNode(start, start.getSize()), query);
		} else {
			return (bpLeafNode) readChildNode(start, start.getSize());
		}
	}
	
	// Used to check nodes just before matches of keys to see if there are any matches there
	// Needed due to possibility of duplicate keys
	private boolean scanLeaf(bpLeafNode node, String query) {
		for(int i = 0; i < node.getSize(); i++) {
			if(node.getKey(i).compareTo(query) == 0) {
				return true;
			}
		}
		return false;
	}
	
	private bpLeafNode readNextLeaf(bpLeafNode node) {
		if(node.getNextLeafOffset() != -1) {
			if(node.getNextLeaf() != null) {
				// Already in memory
				System.err.println("Going to: " + node.getNextLeaf().getKey(0));
				return node.getNextLeaf();
			} else {
				bpLeafNode leafNode = new bpLeafNode(nodeMaxSize);
				leafNode.setOffset(node.getNextLeafOffset());
				try {
					bpFile.seek(leafNode.getOffset() + 1);
					readLeafNode(leafNode);
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
				node.setNextLeaf(leafNode);
				
				return leafNode;
			}
		}
		return null;
	}
	
	private bpNode readChildNode(bpIndexNode parent, int childIndex) {
		bpNode child;
		if(parent.getChildOffset(childIndex) != -1) {
			child = parent.getChild(childIndex);
			
			if(child != null) {
				// Already in memory
				return child;
			} else {
				try {
					bpFile.seek(parent.getChildOffset(childIndex));
					
					// Read in type of node
					if(bpFile.readBoolean()) {
						child = new bpLeafNode(nodeMaxSize);
						readLeafNode(child);
					} else {
						child = new bpIndexNode(nodeMaxSize);
						readIndexNode(child);
					}
					
					child.setOffset(parent.getChildOffset(childIndex));
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

				child.setParent(parent);
				parent.setChild(childIndex, child);
				return child;
			}
		}
		return null;
	}
	
	private bpIndexNode readParentNode(bpNode node) {
		if(node.getParentOffset() != -1) {
			if(node.getParent() != null) {
				// Already in memory
				return node.getParent();
			} else {
				bpIndexNode parent = new bpIndexNode(nodeMaxSize);
				parent.setOffset(node.getParentOffset());
				
				try {
					bpFile.seek(parent.getOffset() + 1); // Skip type boolean
					readIndexNode(parent);
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
				
				node.setParent(parent);
				return parent;				
			}
		}
		return null;
	}
	
	private void readIndexNode(bpNode node) throws IOException {
		byte[] data;
		byte[] intData;
		bpIndexNode indexNode = (bpIndexNode) node;
		
		// Read node data
		data = readNodeCommon(node, false);

		// Process binary data of index node specific data
		for(int i = 0; i < node.getSize(); i++) {
			intData = Arrays.copyOfRange(data, i * INT_SIZE, (i + 1) * INT_SIZE);
			indexNode.setChildOffset(i, ByteBuffer.wrap(intData).getInt());
		}
		intData = Arrays.copyOfRange(data, node.getSize() * INT_SIZE, (node.getSize() + 1) * INT_SIZE);
		indexNode.setChildOffset(node.getSize(), ByteBuffer.wrap(intData).getInt());
	}
	
	private void readLeafNode(bpNode node) throws IOException {
		byte[] data;
		byte[] intData;
		bpLeafNode leafNode = (bpLeafNode) node;
		
		numLeavesRead++;
		
		// Read node data
		data = readNodeCommon(node, true);
		
		// Process binary data of leaf node specific data
		for(int i = 0; i < node.getSize(); i++) {
			intData = Arrays.copyOfRange(data, 2 * i * INT_SIZE, (2 * i + 1) * INT_SIZE);
			leafNode.setKeyPageID(i, ByteBuffer.wrap(intData).getInt());
			
			intData = Arrays.copyOfRange(data, (2 * i + 1) * INT_SIZE, (2 * i + 2) * INT_SIZE);
			leafNode.setKeySlotID(i, ByteBuffer.wrap(intData).getInt());
		}
		intData = Arrays.copyOfRange(data, 2 * node.getSize() * INT_SIZE, (2 * node.getSize() + 1) * INT_SIZE);
		leafNode.setNextLeafOffset(ByteBuffer.wrap(intData).getInt());
		
		intData = Arrays.copyOfRange(data, (2 * node.getSize() + 1) * INT_SIZE, (2 * node.getSize() + 2) * INT_SIZE);
		leafNode.setPrevLeafOffset(ByteBuffer.wrap(intData).getInt());
	}
	
	// Reads in data common to both index and leaf nodes
	// Returns remaining binary data for leaf / index node
	private byte[] readNodeCommon(bpNode node, boolean leaf) throws IOException {
		int keyLength;
		byte[] keyData;
		byte[] valueData;
		byte[] remainingData;
		String key;
		
		// Read in binary data of entire node, also split off leaf/index specific data
		if(leaf) {
			keyData = new byte[2 * INT_SIZE + (INT_SIZE + MAX_FIELD_SIZE) * nodeMaxSize + (2 * nodeMaxSize + 2) * INT_SIZE];
			bpFile.read(keyData);
			remainingData = Arrays.copyOfRange(keyData, 2 * INT_SIZE + (INT_SIZE + MAX_FIELD_SIZE) * nodeMaxSize,
														2 * INT_SIZE + (INT_SIZE + MAX_FIELD_SIZE) * nodeMaxSize + (2 * nodeMaxSize + 2) * INT_SIZE);
		} else {
			keyData = new byte[2 * INT_SIZE + (INT_SIZE + MAX_FIELD_SIZE) * nodeMaxSize + (nodeMaxSize + 1) * INT_SIZE];
			bpFile.read(keyData);
			remainingData = Arrays.copyOfRange(keyData, 2 * INT_SIZE + (INT_SIZE + MAX_FIELD_SIZE) * nodeMaxSize,
														2 * INT_SIZE + (INT_SIZE + MAX_FIELD_SIZE) * nodeMaxSize + (nodeMaxSize + 1) * INT_SIZE);
		}

		// Read in parentOffset and Size
		valueData = Arrays.copyOfRange(keyData, 0, INT_SIZE);
		node.setSize(ByteBuffer.wrap(valueData).getInt());
		valueData = Arrays.copyOfRange(keyData, INT_SIZE, 2 * INT_SIZE);
		node.setParentOffset(ByteBuffer.wrap(valueData).getInt());
		
		// Reduce key data array (leave out leaf/index specific data)
		keyData = Arrays.copyOfRange(keyData, 2 * INT_SIZE , 2 * INT_SIZE + (INT_SIZE + MAX_FIELD_SIZE) * nodeMaxSize);
		
		// Process binary data
		for(int i = 0; i < node.getSize(); i++) {
			// Read in key length
			valueData = Arrays.copyOfRange(keyData, i * (INT_SIZE + MAX_FIELD_SIZE), i * (INT_SIZE + MAX_FIELD_SIZE) + INT_SIZE);
			keyLength = ByteBuffer.wrap(valueData).getInt();
			
			// Read in key
			valueData = Arrays.copyOfRange(keyData, i * (INT_SIZE + MAX_FIELD_SIZE) + INT_SIZE, i * (INT_SIZE + MAX_FIELD_SIZE) + INT_SIZE + keyLength);
			key = new String(valueData);
			
			// Read in key binary data
			node.setKey(i, key);
		}
		return remainingData;
	}
	
	private void writeNode(bpNode node) {
		try {
			// Check if a new node is being written or updating an existing none
			if(node.getOffset() == -1) {
				node.setOffset((int) bpFile.length());
				bpFile.seek(bpFile.length());
				bpFile.setLength(bpFile.length() + nodeByteSize);
			} else {
				bpFile.seek(node.getOffset());
			}
			
			// Write out type of node (0 = Leaf, 1 = Index)
			if(node instanceof bpLeafNode) {
				bpFile.writeBoolean(true);
			} else {
				bpFile.writeBoolean(false);
			}
			
			// Write out parentOffset and Size
			bpFile.writeInt(node.getSize());
			bpFile.writeInt(node.getParentOffset());
			
			// Write out keys
			for(int i = 0; i < node.getSize(); i++) {
				String key = node.getKey(i);
				byte[] keyData;
				
				// Write out key length (number of bytes in string)
				keyData = key.getBytes();
				bpFile.writeInt(keyData.length);
				
				// Write out key
				bpFile.write(keyData);
				
				// Seek to next key position
				bpFile.skipBytes(MAX_FIELD_SIZE - keyData.length);
			}
			
			// Skip empty keys
			bpFile.skipBytes((INT_SIZE + MAX_FIELD_SIZE) * (nodeMaxSize - node.getSize()));
			
			// Write index / leaf node specific data
			if(node instanceof bpLeafNode) {
				bpLeafNode leafNode = (bpLeafNode) node;
				
				for(int i = 0; i < node.getSize(); i++) {
					bpFile.writeInt(leafNode.getKeyPageID(i));
					bpFile.writeInt(leafNode.getKeySlotID(i));
				}
				// Skip empty keys
				bpFile.skipBytes(2 * INT_SIZE * (nodeMaxSize - node.getSize()));
				
				bpFile.writeInt(leafNode.getNextLeafOffset());
				bpFile.writeInt(leafNode.getPrevLeafOffset());
			} else {
				bpIndexNode indexNode = (bpIndexNode) node;
				
				for(int i = 0; i < node.getSize(); i++) {
					bpFile.writeInt(indexNode.getChildOffset(i));
				}
				bpFile.writeInt(indexNode.getChildOffset(node.getSize())); // N+1 pointers
				
				// Skip empty keys
				bpFile.skipBytes(INT_SIZE * (nodeMaxSize - node.getSize()));
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private int ceilingDivision(int x, int y) {
		return (int) Math.ceil(1.0 * x / y);
	}
	
	public void close() {
		try {
			bpFile.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void updateRoot(bpIndexNode root) {
		writeNode(root);
		try {
			bpFile.seek(INT_SIZE);
			bpFile.writeInt(root.getOffset());
			bpFile.seek(INT_SIZE);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public long getHeapFileReadTime() {
		return heapFileReadTime;
	}
	
	public int getNumLeavesRead() {
		return numLeavesRead;
	}
}
