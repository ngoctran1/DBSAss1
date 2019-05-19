package bpIndexLib;

import java.io.BufferedReader;

public class bpTree {
	private bpIndexNode root;
	private bpLeafNode firstLeaf;
	private int nodeSize;
	
	// Bulk Load
	public bpTree(BufferedReader inputData, int nodeSize) {
		this.nodeSize = nodeSize;
		root = new bpIndexNode(nodeSize);
		String[] dataLine = null;
		boolean setFirstPointer = false;
		int numLeafNodes = 0;
		bpLeafNode newLeafNode = null;
		bpLeafNode prevLeaf = null;
		
		outer:while(true) {
			newLeafNode = new bpLeafNode(nodeSize);
			
			// Create Leaf Node
			for(int i = 0; i < nodeSize; i++) {
				try {
					dataLine = inputData.readLine().split(",");
				} catch (Exception e) {
					break outer;
				}
				newLeafNode.addKey(dataLine[0], Integer.parseInt(dataLine[1]), Integer.parseInt(dataLine[2]));
			}
			
			// Add leaf node to tree
			if(!setFirstPointer) {
				insertFirstNode(newLeafNode);
				firstLeaf = newLeafNode;
				setFirstPointer = true;
				prevLeaf = firstLeaf;
				newLeafNode = null;
			} else {
				insertLeafNode(newLeafNode);
				prevLeaf.setNextLeaf(newLeafNode);
				newLeafNode.setPrevLeaf(prevLeaf);
				prevLeaf = newLeafNode;
				newLeafNode = null;
			}
			numLeafNodes++;
		}
		if(newLeafNode != null && newLeafNode.getSize() > 0) {
			if(!setFirstPointer) {
				insertFirstNode(newLeafNode);
				firstLeaf = newLeafNode;
			} else {
				insertLeafNode(newLeafNode);
				prevLeaf.setNextLeaf(newLeafNode);
				newLeafNode.setPrevLeaf(prevLeaf);
			}
		}
		System.err.println("B+ Tree Created");
		System.err.println("Num Leaf Nodes = " + numLeafNodes);
//		printChildren(root);
//		printChildren((bpIndexNode)root.getChild(0));
	}
	
	private void insertFirstNode(bpLeafNode node) {
		root.setChild(0, node);
		node.setParentNode(root);
	}
	
	private void insertLeafNode(bpLeafNode node) {
		bpIndexNode currentNode = root;
		bpIndexNode destNode = null;
		
		// Traverse down to bottom-right most non-leaf node
		destNode = traverseDown(currentNode, node.getKey(0));
		
		if(destNode.getSize() < nodeSize) {
			// If node has free space, assign <P, K> into non-leaf node
			destNode.addKey(node.getKey(0), node);
		} else {
			System.out.println("-------------------------------------------------------");
			System.out.println("FULL NODE:");
			printChildren(destNode);
			
			System.out.println("\nVALUE TO ADD:");
			System.out.println(node.getKey(0));
			
			// Else if node is full, split node and push value up tree
			bpIndexNode newNode = new bpIndexNode(nodeSize);
			
			// Copy half of values into new node
			int copyLength = (destNode.getSize() / 2) - 1;
			int pushValueIndex = (destNode.getSize() / 2) + 1;
			System.out.println("PushValueIndex = " + pushValueIndex);
			System.out.println("\nVALUE TO PUSH:");
			bpNode pushNode = destNode.getChild(pushValueIndex + 1);
			if(pushNode instanceof bpIndexNode) {
				System.out.println(((bpIndexNode)pushNode).getKey(0));
			} else {
				System.out.println( ((bpLeafNode)pushNode).getKey(0) );
			}
			
			// Copy rightmost pointer of old node to first pointer of new node
			newNode.setChild(0, destNode.getChild(pushValueIndex + 1));
			
			// Remember value to be pushed
			String pushValue = destNode.getKey(pushValueIndex);
			
			// Move over half of data
			for(int i = destNode.getSize() - copyLength; i < destNode.getSize(); i++) {
				newNode.addKey(destNode.getKey(i), destNode.getChild(i));
			}
			
			// Remove copied data (another loop is needed since arrayList remove shifts values backwards)
			for(int i = destNode.getSize() - (copyLength + 1); i < destNode.getSize(); i++) {
				destNode.removeKey(i);
			}
			newNode.addKey(node.getKey(0), node);
			
			System.out.println("\nOLD NODE AFTER COPY:");
			printChildren(destNode);
			
			System.out.println("\nNEW NODE AFTER COPY:");
			printChildren(newNode);
			
			// Push middle value up tree
			if(destNode.getParentNode() == null) {
				// Needs new root
				bpIndexNode newRoot = new bpIndexNode(nodeSize);
				newRoot.setChild(0, destNode);
				destNode.setParentNode(newRoot);
				root = newRoot;
				pushUp(newRoot, newNode, pushValue);
			} else {
				pushUp(destNode.getParentNode(), newNode, destNode.getKey(pushValueIndex));
			}
			System.out.println("-------------------------------------------------------");
		}
		node.setParentNode(destNode);
	}
	
	private void pushUp(bpIndexNode start, bpIndexNode prevNewNode, String key) {
		if(start.getSize() == nodeSize) {
			System.out.println("-------------------------------------------------------");
			System.out.println("FULL NODE:");
			printChildren(start);
			
			System.out.println("\nNEW VALUE:");
			System.out.println(key);
			// Split and push up
			bpIndexNode newNode = new bpIndexNode(nodeSize);
			
			// Copy half of values into new node
			int copyLength = (start.getSize() / 2) - 1; // Second half - Key to push up
			int pushValueIndex = copyLength;
			
			// Preserve pointer of the value about to be pushed
			newNode.setChild(0, start.getChild(pushValueIndex));
			
			// Move over half of data
			for(int i = start.getSize() - copyLength; i < start.getSize(); i++) {
				newNode.addKey(start.getKey(i), start.getChild(i));
			}
			
			// Remove copied data (another loop is needed since arrayList remove shifts values backwards
			for(int i = start.getSize() - copyLength; i < start.getSize(); i++) {
				start.removeKey(i);
			}
			newNode.addKey(start.getKey(0), start);
			
			System.out.println("\nOLD NODE AFTER COPY:");
			printChildren(start);
			
			System.out.println("NEW NODE AFTER COPY:");
			printChildren(newNode);
			
			// Push middle value up tree
			if(start.getParentNode() == null) {
				// Needs new root
				bpIndexNode newRoot = new bpIndexNode(nodeSize);
				newRoot.setChild(0, start);
				start.setParentNode(newRoot);
				root = newRoot;
				pushUp(newRoot, newNode, start.getKey(pushValueIndex));
			} else {
				pushUp(start.getParentNode(), newNode, start.getKey(pushValueIndex));
			}
		} else {
			start.addKey(key, prevNewNode);
			prevNewNode.setParentNode(start);
		}
		System.out.println("-------------------------------------------------------");
	}
	
	// Find the bottom-most index node into which a leaf node should be inserted
	private bpIndexNode traverseDown(bpIndexNode startNode, String key) {
		// If next nodes are leaf nodes return this node
		if(startNode.getChild(0) instanceof bpLeafNode) {
			return startNode;
		}
		
		bpIndexNode child = (bpIndexNode) startNode.getChild(0);
		// Search through node to find next node to search
		for(int i = 0; i < child.getSize(); i++) {
			// else find next node to search
			if(child.getKey(i) == null || child.getKey(i).compareTo(key) > 0) {
				traverseDown(child, key);
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
//		System.out.println("COMPARING TO LEAF NODE");
		System.out.println("\nMATCHING RESULT:");
		for(int i = 0; i < result.getSize(); i++) {
//			System.out.println("Comparing to: " + result.getKey(i));
			if(result.getKey(i).compareTo(query) == 0) {
				System.out.println("PageID: " + result.getKeyPageID(i) + ", SlotID: " + result.getKeySlotID(i));
				matched = true;
				if(!findAll) {
					return;
				}
			} else {
				if(matched == true) {
					// No more matches to be found
					return;
				}
				matched = false;
			}
		}
		findRemainMatches(result, query);
	}
	
	private void findRemainMatches(bpLeafNode startNode, String query) {
		bpLeafNode nextNode = startNode.getNextLeaf();
		if(nextNode == null) {
//			System.out.println("NextNode = NULL");
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
//		System.out.println("-------------------------------------------------------");
//		if(start.getSize() == 0) {
//			System.out.println("Searching Leaf Node: " + (((bpLeafNode)start.getChild(0)).getKey(0)) + " for query: " + query);
//		} else {
//			System.out.println("Searching Index Node: " + start.getKey(0) + " for query: " + query);
//		}
//		printChildren(start);
		for(int i = 0; i < start.getSize(); i++) {
//			System.out.println("Comparing to: " + start.getKey(i));
			if(start.getKey(i).compareTo(query) >= 0) { // Includes equality due to duplicates
				if(start.getChild(i) instanceof bpIndexNode) {
//					System.out.println("Traversing into K" + i + ", K" + (i + 1) + " = P" + i);
					bpLeafNode potentialResult = traverseQuery((bpIndexNode)start.getChild(i), query);
					if(potentialResult != null) {
						return potentialResult;
					} else {
						return potentialResult = traverseQuery((bpIndexNode)start.getChild(i + 1), query);
					}
				} else {
//					System.out.println("Returning Leaf");
					bpLeafNode potentialResult = (bpLeafNode)start.getChild(i);
					
					// Check whether the node just before matching key contains query
					// If not, move onto next node. Needed due to duplicates
					if(scanLeaf(potentialResult, query)) {
						return potentialResult;
					} else {
						return (bpLeafNode)potentialResult.getNextLeaf();
					}
				}
			}
		}
		if(start.getChild(start.getSize()) instanceof bpIndexNode) {
			traverseQuery((bpIndexNode)start.getChild(start.getSize()), query);
		} else {
			return (bpLeafNode)start.getChild(start.getSize());
		}
//		System.out.println("-------------------------------------------------------");
		return null;
	}
	
	// Used to check nodes just before matches of keys to see if there are any matches there
	// Needed due to possibility of duplicates
	private boolean scanLeaf(bpLeafNode node, String query) {
		for(int i = 0; i < node.getSize(); i++) {
//			System.out.println("Scnaning: " + node.getKey(i));
			if(node.getKey(i).compareTo(query) == 0) {
				return true;
			}
		}
		return false;
	}
	
	public void printLeaves() {
		bpLeafNode node = firstLeaf;
		bpLeafNode next = node.getNextLeaf();
		
		while(next != null) {
			System.out.println(node.getKey(0));
			node = node.getNextLeaf();
			next = next.getNextLeaf();
		}
		System.out.println(node.getKey(0));
	}
	
	public void printChildren(bpIndexNode node) {
		// Only contains a single leaf node
//		if(node.getSize() == 0) {
//			bpLeafNode leaf = (bpLeafNode)node.getChild(0);
//			for(int i = 0; i < leaf.getSize(); i++) {
//				System.out.println( "V"+ i +"= " + leaf.getKey(i));
//			}
//		}
		for(int i = 0; i <= node.getSize(); i++) {
			if(node.getChild(i) instanceof bpIndexNode) {
				bpIndexNode child = (bpIndexNode)node.getChild(i);
				System.out.println( "P"+ i +": bpIndexNode = " + child.getKey(0));
			} else {
				bpLeafNode child = (bpLeafNode)node.getChild(i);
				System.out.println( "P"+ i +": bpLeafNode = " + child.getKey(0));
			}
			if(i != node.getSize()) {
				System.out.println("K" + (i+1) + ": " + node.getKey(i));
			}
		}
		System.out.println();
	}
}
