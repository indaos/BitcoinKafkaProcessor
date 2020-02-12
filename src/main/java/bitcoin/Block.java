package bitcoin;

import java.util.ArrayList;

public class Block {
    private int version;
    private byte[] prevHash;
    private byte[] rootHash;
    private int time;
    private int nBits;
    private int nonce;
    private ArrayList<BitTransaction> trans=new ArrayList<>();

    public Block() {

    }

    public ArrayList<BitTransaction> getTransactions() {
        return  trans;
    }

    public void addTransaction(BitTransaction t){
        trans.add(t);
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public byte[] getPrevHash() {
        return prevHash;
    }

    public void setPrevHash(byte[] prevHash) {
        this.prevHash = prevHash;
    }

    public byte[] getRootHash() {
        return rootHash;
    }

    public void setRootHash(byte[] rootHash) {
        this.rootHash = rootHash;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public int getnBits() {
        return nBits;
    }

    public void setnBits(int nBits) {
        this.nBits = nBits;
    }

    public int getNonce() {
        return nonce;
    }

    public void setNonce(int nonce) {
        this.nonce = nonce;
    }

    public String toString() {
       return new StringBuilder()
          .append(version).append("block:[,")
          .append(new java.util.Date(time)).append(",")
          .append(Utils.toHexString(prevHash)).append(",")
          .append(Utils.toHexString(rootHash)).append("]")
          .toString();
    }
}
