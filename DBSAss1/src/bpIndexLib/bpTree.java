package bpIndexLib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;

public class bpTree {
	private static final int INT_SIZE = 4;
	private static final int CHAR_SIZE = 2;
	private static final int MAX_FIELD_SIZE = 50;

	// Leaf nodes are bigger than index nodes. They contain (2N+6) INT and N STR where N number of keys
	private int nodeByteSize; 
	private bpIndexNode root;
	private bpLeafNode firstLeaf;
	private bpLeafNode lastLeaf;
	private int nodeMaxSize;
	private RandomAccessFile bpFile;
	
	// Read pre-existing B+ tree
	public bpTree(RandomAccessFile bpFile) {
		this.bpFile = bpFile;
		
		try {
			// Read in node size
			nodeMaxSize = bpFile.readInt();
			nodeByteSize = (2 * nodeMaxSize + 6) * INT_SIZE + nodeMaxSize * (MAX_FIELD_SIZE + 1) + 2;
			System.out.println("nodeByteSize = " + nodeByteSize);
			System.out.println("Max Node Size: " + nodeMaxSize + ", Pointer at " + bpFile.getFilePointer());
			
			root = new bpIndexNode(nodeMaxSize);
			root.setOffset(bpFile.readInt());
			System.out.println("rootOffset: " + root.getOffset());
			
			firstLeaf = new bpLeafNode(nodeMaxSize);
			
			firstLeaf.setOffset((int)bpFile.getFilePointer());
			System.out.println("Leaf Node? " + bpFile.readBoolean() + ", Pointer at " + bpFile.getFilePointer());
			readLeafNode(firstLeaf);
			
			bpFile.seek(root.getOffset());
			System.out.println("Leaf Node? " + bpFile.readBoolean() + ", Pointer at " + bpFile.getFilePointer());
			readIndexNode(root);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	// Bulk Load
	public bpTree(BufferedReader inputData, int nodeMaxSize) {
		nodeByteSize = (2 * nodeMaxSize + 6) * INT_SIZE + nodeMaxSize * (MAX_FIELD_SIZE + 1) + 2;
		System.out.println("nodeByteSize = " + nodeByteSize);
		this.nodeMaxSize = nodeMaxSize;
		root = new bpIndexNode(nodeMaxSize);
		String[] dataLine = null;
		boolean setFirstPointer = false;
		int numLeafNodes = 0;
		bpLeafNode newLeafNode = null;
		
		// Create new index file
		try {
			bpFile = new RandomAccessFile("bpIndex." + nodeMaxSize, "rw");
			bpFile.setLength(2 * INT_SIZE);
			
			// Header of file contains how many records each node contains and where the root is
			bpFile.writeInt(nodeMaxSize);
			bpFile.writeInt(root.getOffset());
			
			System.out.println("Node Max Size = " + nodeMaxSize);
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
//				System.out.println("Setting next leaf for " + lastLeaf.getKey(0) + ": " + newLeafNode.getOffset() + " (" + newLeafNode.getKey(0) + ")");
				writeNode(lastLeaf);
				
				newLeafNode.setPrevLeaf(lastLeaf);
//				System.out.println("Setting prev leaf for " + newLeafNode.getKey(0) + ": " + lastLeaf.getOffset() + " (" + lastLeaf.getKey(0) + ")");
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
				System.out.println("Setting next leaf for " + lastLeaf.getKey(0) + ": " + newLeafNode.getOffset() + " (" + newLeafNode.getKey(0) + ")");
				writeNode(lastLeaf);
				
				newLeafNode.setPrevLeaf(lastLeaf);
				System.out.println("Setting prev leaf for " + newLeafNode.getKey(0) + ": " + lastLeaf.getOffset() + " (" + lastLeaf.getKey(0) + ")");
				writeNode(newLeafNode);
				
				lastLeaf = newLeafNode;
			}
		}
		
		System.err.println("B+ Tree Created");
		System.err.println("Num Leaf Nodes = " + numLeafNodes);
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
//			System.out.println("-------------------------------------------------------");
//			System.out.println("FULL NODE:");
//			printChildren(pushIntoNode);
//			
//			System.out.println("\nNEW VALUE:");
//			System.out.println(newKey);
//			System.out.println(newKeyNode.getKey(0));
			
			bpIndexNode newNode = new bpIndexNode(nodeMaxSize);
			
			int startCopyIndex = ceilingDivision(pushIntoNode.getSize(), 2) + 1;
			int copyLength = (pushIntoNode.getSize() / 2) - 1; // Second half - Key to push up
			int pushValueIndex = startCopyIndex - 1;
			
			// Preserve pointer of the value about to be pushed by storing it in the first pointer in new node
			String pushKey = pushIntoNode.getKey(pushValueIndex);
			
//			System.out.println("Node Size: " + pushIntoNode.getSize() + ", " + "startCopyIndex = " + startCopyIndex);
			newNode.setChild(0, readChildNode(pushIntoNode, startCopyIndex));
			for(int i = 0; i < copyLength; i++) {
				newNode.addKey(pushIntoNode.getKey(startCopyIndex + i), readChildNode(pushIntoNode, startCopyIndex + i + 1));
			}
			newNode.addKey(newKey, newKeyNode);
			
			for(int i = 0; i < copyLength + 1; i++) {
				pushIntoNode.removeKey(pushValueIndex);
			}
			
//			System.out.println("\nOLD NODE AFTER COPY:");
//			printChildren(pushIntoNode);
//			
//			System.out.println("\nNEW NODE AFTER COPY:");
//			printChildren(newNode);
			
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
			// No need to push up
			pushIntoNode.addKey(newKey, newKeyNode);
			newKeyNode.setParent(pushIntoNode);
			
			writeNode(pushIntoNode);
			writeNode(newKeyNode);
		}
//		System.out.println("-------------------------------------------------------");
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
	
	public void equalityQuery(String query, boolean findAll) {
		System.out.println("\nQuery:");
		System.out.println(query);
		bpIndexNode currentNode = root;
		bpLeafNode result = traverseQuery(currentNode, query);
		boolean matched = false;
		System.out.println("\nMATCHING RESULT:");
		for(int i = 0; i < result.getSize(); i++) {
			if(result.getKey(i).compareTo(query) == 0) {
				System.out.println("PageID: " + result.getKeyPageID(i) + ", SlotID: " + result.getKeySlotID(i));
				matched = true;
				if(!findAll) {
					return;
				}
			} else {
				if(matched == true) {
					// No more matches to be found due to sorted linked list
					System.out.println("Found all values?");
					return;
				}
				matched = false;
			}
		}
		findRemainMatches(result, query);
	}
	
	public int rangeQuery(String lowBound, String highBound) {
		boolean matched = false;
		boolean brokenChain = false;
		int numMatches = 0;
		System.out.println("\nRANGE QUERY: " + lowBound + " TO " + highBound);
		// Invalid inputs
		if(lowBound.compareTo(highBound) > 0) {
			return numMatches;
		}
		
		bpIndexNode currentNode = root;
		bpLeafNode result = traverseQuery(currentNode, lowBound);
		
		while(result != null) {
			for(int i = 0; i < result.getSize(); i++) {
				if(result.getKey(i).compareTo(lowBound) >= 0 && result.getKey(i).compareTo(highBound) < 0) {
//					System.out.println("Key = " + result.getKey(i) + ", PageID = " + result.getKeyPageID(i) + ", SlotID = " + result.getKeySlotID(i));
					matched = true;
					numMatches++;
				} else if (matched == true) {
					System.out.println("Broken by: " + result.getKey(i));
					brokenChain = true;
					break;
				}
			}
			if(brokenChain == true) {
				break;
			} else {
				result = readNextLeaf(result);
			}
		}
		return numMatches;
	}
	
	private void findRemainMatches(bpLeafNode startNode, String query) {
//		bpLeafNode nextNode = startNode.getNextLeaf();
		bpLeafNode nextNode = readNextLeaf(startNode);
		if(nextNode == null) {
			System.out.println("No next leaf");
			return;
		}
		for(int i = 0; i < nextNode.getSize(); i++) {
			if(nextNode.getKey(i).compareTo(query) != 0) {
				return;
			} else {
				System.out.println("PageID: " + nextNode.getKeyPageID(i) + ", SlotID: " + nextNode.getKeySlotID(i));
			}
		}
		findRemainMatches(nextNode, query);
	}
	
	private bpLeafNode traverseQuery(bpIndexNode start, String query) {
//		if(start.getSize() == 0) {
//			System.out.println("Searching Leaf Node: " + (((bpLeafNode)start.getChild(0)).getKey(0)) + " for query: " + query);
//		} else {
//			System.out.println("Searching Index Node: " + start.getKey(0) + " for query: " + query);
//		}
//		printChildren(start);
//		for(int i = 0; i < start.getSize(); i++) {
//			System.out.println(i + ": " + start.getChildOffset(i));
//		}
//		System.out.println(start.getSize() + ": " + start.getChildOffset(start.getSize()));
		
		for(int i = 0; i < start.getSize(); i++) {
			if(start.getKey(i).compareTo(query) >= 0) { // Includes equality due to duplicates
				System.out.println("Found at " + i);
				if(readChildNode(start, i) instanceof bpIndexNode) {
					System.out.println("reading index child");
					bpLeafNode potentialResult = traverseQuery((bpIndexNode)readChildNode(start, i), query);
					if(potentialResult != null) {
						return potentialResult;
					} else {
						return potentialResult = traverseQuery((bpIndexNode)readChildNode(start, i + 1), query);
					}
				} else {
					System.out.println("reading leaf child");
					bpLeafNode potentialResult = (bpLeafNode)readChildNode(start, i);
					
					// Check whether the node just before matching key contains query
					// If not, move onto next node. Needed due to duplicates
					if(scanLeaf(potentialResult, query)) {
						System.out.println("Reading potential result");
						return potentialResult;
					} else {
//						return (bpLeafNode)potentialResult.getNextLeaf();
						System.out.println("Reading next leaf");
						return (bpLeafNode)readNextLeaf(potentialResult);
					}
				}
			}
		}
		
		// No keys match, so traverse the last pointer
		if(readChildNode(start, start.getSize()) instanceof bpIndexNode) {
			traverseQuery((bpIndexNode)readChildNode(start, start.getSize()), query);
		} else {
			return (bpLeafNode)readChildNode(start, start.getSize());
		}
		return null;
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
	
	public void printLeaves() {
		bpLeafNode node = firstLeaf;
//		bpLeafNode next = node.getNextLeaf();
		bpLeafNode next = readNextLeaf(node);
		
		while(next != null) {
//			System.out.println("NODE: " + node.getKey(0));
			for(int i = 0; i < node.getSize(); i++) {
				System.out.println(i + ": " + node.getKey(i));
			}
//			node = node.getNextLeaf();
//			next = next.getNextLeaf();
			node = readNextLeaf(node);
			next = readNextLeaf(next);
		}
//		System.out.println(node.getKey(0));
		for(int i = 0; i < node.getSize(); i++) {
			System.out.println(i + ": " + node.getKey(i));
		}
	}
	
	public void printChildren(bpIndexNode node) {
		for(int i = 0; i <= node.getSize(); i++) {
			if(readChildNode(node, i) instanceof bpIndexNode) {
				bpIndexNode child = (bpIndexNode)readChildNode(node, i);
				System.out.println( "P"+ i +": bpIndexNode = " + child.getKey(0));
			} else {
				bpLeafNode child = (bpLeafNode)readChildNode(node, i);
				System.out.println( "P"+ i +": bpLeafNode = " + child.getKey(0));
			}
			if(i != node.getSize()) {
				System.out.println("K" + (i+1) + ": " + node.getKey(i));
			}
		}
		System.out.println();
	}
	
	public int getHeight(bpNode start, int currentHeight) {
		if(start == root) {
			return currentHeight;
		} else {
			return getHeight(readParentNode(start), currentHeight + 1);
		}
	}
	
	private bpLeafNode readNextLeaf(bpLeafNode node) {
		if(node.getNextLeafOffset() != -1) {
			if(node.getNextLeaf() != null) {
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
				return child;
			} else {
				try {
					bpFile.seek(parent.getChildOffset(childIndex));
					
					// Read in type of node
					if(bpFile.readBoolean()) {
//						System.out.println("Leaf Node");
						child = new bpLeafNode(nodeMaxSize);
						readLeafNode(child);
					} else {
//						System.out.println("Index Node");
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
				// TODO: Link Parent to child
				return parent;				
			}
		}
		return null;
	}
	
	// Assumes offset has been set
	private void readIndexNode(bpNode node) throws IOException {
		readNodeCommon(node);

		for(int i = 0; i < node.getSize(); i++) {
			int childOffset = bpFile.readInt();
			((bpIndexNode)node).setChildOffset(i, childOffset);
		}
		int childOffset = bpFile.readInt();
		((bpIndexNode)node).setChildOffset(node.getSize(), childOffset);
		bpFile.seek(node.getOffset() + nodeByteSize);
	}
	
	// Assumes offset has been set
	private void readLeafNode(bpNode node) throws IOException {
		readNodeCommon(node);
		int value;
		for(int i = 0; i < node.getSize(); i++) {
			value = bpFile.readInt();
			((bpLeafNode) node).setKeyPageID(i, value);
			value = bpFile.readInt();
			((bpLeafNode) node).setKeySlotID(i, value);
		}
		value = bpFile.readInt();
		((bpLeafNode) node).setNextLeafOffset(value);
//		System.out.println("Next Leaf Offset = " + value);
		value = bpFile.readInt();
		((bpLeafNode) node).setPrevLeafOffset(value);
//		System.out.println("Prev Leaf Offset = " + value);
		
		bpFile.seek(node.getOffset() + nodeByteSize);
	}
	
	private void readNodeCommon(bpNode node) throws IOException {
		// Read in parentOffset and Size
		int size = bpFile.readInt();
		node.setSize(size);
		int pare = bpFile.readInt();
		node.setParentOffset(pare);
		
		// Read in Keys
		for(int i = 0; i < node.getSize(); i++) {
			String key = bpFile.readLine().trim();
			node.setKey(i, key);
		}
	}
	
	// Leaf nodes are bigger than index nodes. They contain (2N+4) INT and N STR where N number of keys
	// To avoid shifting data, we assign static size for every node
	private void writeNode(bpNode node) {
		try {
			// Check if a new node is being written or updating an existing none
			if(node.getOffset() == -1) {
//				System.out.println("Old Length = " + bpFile.length() + ", Pointer: " + bpFile.getFilePointer());
				node.setOffset((int) bpFile.length());
				bpFile.seek(bpFile.length());
				bpFile.setLength(bpFile.length() + nodeByteSize);
//				System.out.println("New Length = " + bpFile.length() + ", Pointer: " + bpFile.getFilePointer());
//				System.out.println("Created node at offset: " + node.getOffset());
			} else {
				bpFile.seek(node.getOffset());
//				System.out.println("Existing Node at: " + node.getOffset() + ", Pointer at " + bpFile.getFilePointer());
			}
			
			// Write out type of node (0 = Leaf, 1 = Index)
			if(node instanceof bpLeafNode) {
				bpFile.writeBoolean(true);
			} else {
				bpFile.writeBoolean(false);
			}
			
//			System.out.println("New Length = " + bpFile.length() + ", Pointer: " + bpFile.getFilePointer());
			
			
			// Write out parentOffset and Size
			bpFile.writeInt(node.getSize());
			bpFile.writeInt(node.getParentOffset());
			
//			System.out.println("New Length = " + bpFile.length() + ", Pointer: " + bpFile.getFilePointer());
//			System.exit(0);
			
			// Write out keys (separated by new line character)
			for(int i = 0; i < node.getSize(); i++) {
				String key = node.getKey(i);
				bpFile.writeBytes(key);
				bpFile.writeChars("\n");
			}
			
			// If Leaf Node
				// Write out keyPageID and keySlotID, also write next and prev leaf offsets
			// Else
				// Write out childrenOffset
			if(node instanceof bpLeafNode) {
//				System.out.println("---------------------------------------------");
//				System.out.println("WRITING LEAF POINTER");
//				System.out.println("LeafNode: " + node.getKey(0));
				bpLeafNode leafNode = (bpLeafNode) node;
				
				for(int i = 0; i < node.getSize(); i++) {
					bpFile.writeInt(leafNode.getKeyPageID(i));
					bpFile.writeInt(leafNode.getKeySlotID(i));
				}
				
				bpFile.writeInt(leafNode.getNextLeafOffset());
//				System.out.println("Next Leaf Offset = " + leafNode.getNextLeafOffset());
				bpFile.writeInt(leafNode.getPrevLeafOffset());
//				System.out.println("Prev Leaf Offset = " + leafNode.getPrevLeafOffset());
//				System.out.println("---------------------------------------------");
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
	
	public bpNode getFirstLeaf() {
		return firstLeaf;
	}
	
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
			System.out.println("Setting root offset: " + root.getOffset());
			System.out.println("Writing root offset: " + bpFile.readInt());
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
