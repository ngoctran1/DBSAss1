package bpIndexLib;

public interface bpNode {
	public int getOffset();

	public void setOffset(int offset);
	
	public int getSize();
	
	public void setSize(int size);
	
	public void setParent(bpIndexNode parent);
	
	public bpIndexNode getParent();
	
	public int getParentOffset();
	
	public void setParentOffset(int parentOffset);
	
	public String getKey(int index);
	
	public void setKey(int index, String key);
}
