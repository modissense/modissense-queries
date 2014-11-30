package gr.ntua.ece.cslab.modissense.queries.containers;

public interface Compressible {

	public byte[] getCompressedBytes();
	
	public void parseCompressedBytes(byte[] array);
}
