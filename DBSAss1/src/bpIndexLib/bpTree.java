package bpIndexLib;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

import dbLoadLib.Heap;

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
	
	// Read pre-existing B+ tree
	public bpTree(RandomAccessFile bpFile) {
		this.bpFile = bpFile;
		
		System.out.println("READING B+ INDEX FILE\n");
		try {
			nodeMaxSize = bpFile.readInt();
			nodeByteSize = (2 * nodeMaxSize + 6) * INT_SIZE + nodeMaxSize * (MAX_FIELD_SIZE + 1) + 2;
			
			System.out.println("nodeByteSize = " + nodeByteSize);
			System.out.println("Max Node Size: " + nodeMaxSize);
			
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
			System.exit(0);
		}
	}
	
	// Bulk Load
	public bpTree(BufferedReader inputData, int nodeMaxSize) {
		nodeByteSize = (2 * nodeMaxSize + 6) * INT_SIZE + nodeMaxSize * (MAX_FIELD_SIZE + 1) + 2;
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
			
			// Header of file contains how many records each node contains and where the root is
			bpFile.writeInt(nodeMaxSize);
			bpFile.writeInt(root.getOffset());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		outer:while(true) {	
			newLeafNode = new bpLeafNode(nodeMaxSize);
			// Create Leaf Node
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
				
				// Update leaf nodes
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
		
		// Traverse down to bottom-right most non-leaf node
		destNode = traverseDown(currentNode, node.getKey(0));
		
		// Push key value into this node
		pushUp(destNode, node, node.getKey(0));
		
		node.setParent(destNode);
		
		writeNode(destNode);
	}
	
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
	
	// Find the bottom-most index node into which a leaf node should be inserted
	private bpIndexNode traverseDown(bpIndexNode startNode, String key) {
		// If next nodes are leaf nodes return this node
		if(readChildNode(startNode, 0) instanceof bpLeafNode || readChildNode(startNode, 0) == null) {
			return startNode;
		}
		
		bpIndexNode child = (bpIndexNode) readChildNode(startNode, 0);
		// Search through node to find next node to search
		for(int i = 0; i < child.getSize(); i++) {
			// else find next node to search
			if(child.getKey(i).compareTo(key) > 0) {
				return traverseDown(child, key);
			}
		}
		return startNode;
	}
	
	public void equalityQuery(String query, boolean findAll, Heap heap, BufferedWriter output) {
		bpIndexNode currentNode = root;
		bpLeafNode result = traverseQuery(currentNode, query);
		boolean matched = false;
		for(int i = 0; i < result.getSize(); i++) {
			if(result.getKey(i).compareTo(query) == 0) {
				try {
					output.write(heap.getPage(result.getKeyPageID(i)).getSlotByIndex(result.getKeySlotID(i)).getReadableData());
					output.write("\n");
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
				
				matched = true;
				if(!findAll) {
					return;
				}
			} else {
				if(matched == true) {
					// No more matches to be found due to sorted linked list
					return;
				}
				matched = false;
			}
		}
		findRemainMatches(result, query, heap, output);
	}
	
	public int rangeQuery(String lowBound, String highBound, Heap heap, BufferedWriter output) {
		boolean matched = false;
		boolean brokenChain = false;
		int numMatches = 0;
		int numMismatch;
		
		// Invalid inputs
		if(lowBound.compareTo(highBound) > 0) {
			return numMatches;
		}
		
		bpIndexNode currentNode = root;
		bpLeafNode result = traverseQuery(currentNode, lowBound);
		
		while(result != null) {
			numMismatch = 0;
			for(int i = 0; i < result.getSize(); i++) {
				if(result.getKey(i).compareTo(lowBound) >= 0 && result.getKey(i).compareTo(highBound) < 0) {
					try {
						output.write(heap.getPage(result.getKeyPageID(i)).getSlotByIndex(result.getKeySlotID(i)).getReadableData());
						output.write("\n");
					} catch (IOException e) {
						e.printStackTrace();
						System.exit(0);
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
	
	// Keeps scanning successive leaf nodes until it comes across a key no satisfying query
	private void findRemainMatches(bpLeafNode startNode, String query, Heap heap, BufferedWriter output) {
		bpLeafNode nextNode = readNextLeaf(startNode);
		if(nextNode == null) {
			return;
		}
		for(int i = 0; i < nextNode.getSize(); i++) {
			if(nextNode.getKey(i).compareTo(query) != 0) {
				return;
			} else {
				try {
					output.write(heap.getPage(nextNode.getKeyPageID(i)).getSlotByIndex(nextNode.getKeySlotID(i)).getReadableData());
					output.write("\n");
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
			}
		}
		findRemainMatches(nextNode, query, heap, output);
	}
	
	// Uses query to traverse down tree to leaf nodes that contain the query.
	private bpLeafNode traverseQuery(bpIndexNode start, String query) {
		for(int i = 0; i < start.getSize(); i++) {
			if(start.getKey(i).compareTo(query) >= 0) { // Includes equality due to duplicates
				if(readChildNode(start, i) instanceof bpIndexNode) {
					// Checks to see if subtree before matched key contains query (possible due to duplicates)
					bpLeafNode potentialResult = traverseQuery((bpIndexNode)readChildNode(start, i), query);
					
					if(potentialResult != null) {
						// Go into node before key
						return potentialResult;
					} else {
						// Go into node after key
						return potentialResult = traverseQuery((bpIndexNode) readChildNode(start, i + 1), query);
					}
				} else {
					bpLeafNode potentialResult = (bpLeafNode)readChildNode(start, i);
					
					if(scanLeaf(potentialResult, query)) {
						// Checks if leaf node just before matched key contains query (possible due to duplicates)
						return potentialResult;
					} else {
						// Return leaf node after matched key
						return (bpLeafNode) readNextLeaf(potentialResult);
					}
				}
			}
		}
		
		// No keys match, so traverse the last pointer
		if(readChildNode(start, start.getSize()) instanceof bpIndexNode) {
			return traverseQuery((bpIndexNode)readChildNode(start, start.getSize()), query);
		} else {
			return (bpLeafNode) readChildNode(start, start.getSize());
		}
	}
	
	// Used to check nodes just before matches of keys to see if there are any matches there
	// Needed due to possibility of duplicates
	private boolean scanLeaf(bpLeafNode node, String query) {
		for(int i = 0; i < node.getSize(); i++) {
			if(node.getKey(i).compareTo(query) == 0) {
				return true;
			}
		}
		return false;
	}
	
//	public void printLeaves() {
//		bpLeafNode node = firstLeaf;
////		bpLeafNode next = node.getNextLeaf();
//		bpLeafNode next = readNextLeaf(node);
//		
//		while(next != null) {
////			System.out.println("NODE: " + node.getKey(0));
//			for(int i = 0; i < node.getSize(); i++) {
//				System.out.println(i + ": " + node.getKey(i));
//			}
////			node = node.getNextLeaf();
////			next = next.getNextLeaf();
//			node = readNextLeaf(node);
//			next = readNextLeaf(next);
//		}
////		System.out.println(node.getKey(0));
//		for(int i = 0; i < node.getSize(); i++) {
//			System.out.println(i + ": " + node.getKey(i));
//		}
//	}
	
//	public void printChildren(bpIndexNode node) {
//		for(int i = 0; i <= node.getSize(); i++) {
//			if(readChildNode(node, i) instanceof bpIndexNode) {
//				bpIndexNode child = (bpIndexNode)readChildNode(node, i);
//				System.out.println( "P"+ i +": bpIndexNode = " + child.getKey(0));
//			} else {
//				bpLeafNode child = (bpLeafNode)readChildNode(node, i);
//				System.out.println( "P"+ i +": bpLeafNode = " + child.getKey(0));
//			}
//			if(i != node.getSize()) {
//				System.out.println("K" + (i+1) + ": " + node.getKey(i));
//			}
//		}
//		System.out.println();
//	}
	
//	public int getHeight(bpNode start, int currentHeight) {
//		if(start == root || start.getParentOffset() == -1) {
//			return currentHeight;
//		} else {
//			return getHeight(readParentNode(start), currentHeight + 1);
//		}
//	}
	
	private bpLeafNode readNextLeaf(bpLeafNode node) {
		if(node.getNextLeafOffset() != -1) {
			if(node.getNextLeaf() != null) {
				// Already in memory
				return node.getNextLeaf();
			} else {
				bpLeafNode leafNode = new bpLeafNode(nodeMaxSize);
				leafNode.setOffset(node.getNextLeafOffset());
				try {
					bpFile.seek(leafNode.getOffset() + 1);
					readLeafNode(leafNode);
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
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
					System.exit(0);
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
					System.exit(0);
				}
				
				node.setParent(parent);
				return parent;				
			}
		}
		return null;
	}
	
	private void readIndexNode(bpNode node) throws IOException {
		// Assumes offset has been set
		readNodeCommon(node);

		for(int i = 0; i < node.getSize(); i++) {
			int childOffset = bpFile.readInt();
			((bpIndexNode)node).setChildOffset(i, childOffset);
		}
		int childOffset = bpFile.readInt();
		((bpIndexNode)node).setChildOffset(node.getSize(), childOffset);
	}
	
	private void readLeafNode(bpNode node) throws IOException {
		// Assumes offset has been set for node
		readNodeCommon(node);
		
		for(int i = 0; i < node.getSize(); i++) {
			((bpLeafNode) node).setKeyPageID(i, bpFile.readInt());
			((bpLeafNode) node).setKeySlotID(i, bpFile.readInt());
		}
		((bpLeafNode) node).setNextLeafOffset(bpFile.readInt());
		((bpLeafNode) node).setPrevLeafOffset(bpFile.readInt());
	}
	
	// Reads in data common to both index and leaf nodes
	private void readNodeCommon(bpNode node) throws IOException {
		// Read in parentOffset and Size
		node.setSize(bpFile.readInt());
		node.setParentOffset(bpFile.readInt());
		
		// Read in Keys
		for(int i = 0; i < node.getSize(); i++) {
			String key = bpFile.readLine().trim();
			node.setKey(i, key);
		}
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
			
			// Write out keys (separated by new line character)
			for(int i = 0; i < node.getSize(); i++) {
				String key = node.getKey(i);
				bpFile.writeBytes(key);
				bpFile.writeChars("\n");
			}
			
			// Write index / leaf node specific data
			if(node instanceof bpLeafNode) {
				bpLeafNode leafNode = (bpLeafNode) node;
				
				for(int i = 0; i < node.getSize(); i++) {
					bpFile.writeInt(leafNode.getKeyPageID(i));
					bpFile.writeInt(leafNode.getKeySlotID(i));
				}
				
				bpFile.writeInt(leafNode.getNextLeafOffset());
				bpFile.writeInt(leafNode.getPrevLeafOffset());
			} else {
				bpIndexNode indexNode = (bpIndexNode) node;
				
				for(int i = 0; i < node.getSize(); i++) {
					bpFile.writeInt(indexNode.getChildOffset(i));
				}
				bpFile.writeInt(indexNode.getChildOffset(node.getSize())); // N+1 pointers
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private int ceilingDivision(int x, int y) {
		return (int) Math.ceil(1.0 * x / y);
	}
	
//	public bpNode getFirstLeaf() {
//		return firstLeaf;
//	}
//	
	public void close() {
		try {
			bpFile.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
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
			System.exit(0);
		}
	}
}
