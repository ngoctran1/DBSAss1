package bpIndexLib;
public class sortMerge {
	//	Only works for Strings! Passing in int as Strings will result in lexicographical ordering
	public static void mergeSort(String[] keys, int[] keyPageID, int[] keySlotID, int start, int length) {
		// Base Case
		if(length <= 1) {
			return;
		}
		
		int half1index = start;
		int half2index = start + length / 2;
		
		// Split array in half and recursively perform mergeSort
		mergeSort(keys, keyPageID, keySlotID, half1index, length / 2);
		mergeSort(keys, keyPageID, keySlotID, half2index, length - (length/2));
		
		// Merge two halves
		String[] mergedKeys = new String[length];
		int[] mergedKeyPageID = new int[length];
		int[] mergedKeySlotID = new int[length];
		
		for(int i = 0; i < length; i++) {
			if(keys[half1index].compareTo(keys[half2index]) <= 0) {
				// Half 1 comes first
				mergedKeys[i] = keys[half1index];
				mergedKeyPageID[i] = keyPageID[half1index];
				mergedKeySlotID[i] = keySlotID[half1index];
				half1index++;
			} else {
				// Half 2 comes first
				mergedKeys[i] = keys[half2index];
				mergedKeyPageID[i] = keyPageID[half2index];
				mergedKeySlotID[i] = keySlotID[half2index];
				half2index++;
			}
			// Merge rest of unfinished half
			if(half1index == start + length / 2) {
				i++;
				while(i < length) {
					mergedKeys[i] = keys[half2index];
					mergedKeyPageID[i] = keyPageID[half2index];
					mergedKeySlotID[i] = keySlotID[half2index];
					half2index++;
					i++;
				}
				break;
			} else if(half2index == start + length) {
				i++;
				while(i < length) {
					mergedKeys[i] = keys[half1index];
					mergedKeyPageID[i] = keyPageID[half1index];
					mergedKeySlotID[i] = keySlotID[half1index];
					half1index++;
					i++;
				}
				break;
			}
		}
		
		// Copy over merge results
		for(int i = 0; i < length; i++) {
			keys[start + i] = mergedKeys[i];
			keyPageID[start + i] = mergedKeyPageID[i];
			keySlotID[start + i] = mergedKeySlotID[i];
		}
	}
}
