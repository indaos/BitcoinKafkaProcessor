package bitcoin;

import java.util.ArrayList;

public class BitTransaction {

  private String id;
  private long lockTime = 0;
  private ArrayList<Tuple> inputs = new ArrayList<>();
  private ArrayList<Tuple> outputs = new ArrayList<>();

  public static class Tuple {

    public String address;
    public String txId = "";
    public long value = 0;
    public int txOut = 0;
    public boolean isCoinBase = false;

    Tuple(String address, long value) {
      this.address = address;
      this.value = value;
    }

    Tuple(String address, String txId, int txOut, boolean isCoinBase) {
      this.address = address;
      this.txId = txId;
      this.txOut = txOut;
      this.isCoinBase = isCoinBase;
    }


    public String toString() {
      return new StringBuilder()
          .append("pair:[").append(address).append(",")
          .append(value).append(",")
          .append(txId).append(",")
          .append(txOut).append(",")
          .append("]")
          .toString();
    }
  }


  public BitTransaction() {
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getLockTime() {
    return lockTime;
  }

  public void setLockTime(long lockTime) {
    this.lockTime = lockTime * 1000L;
  }

  public ArrayList<Tuple> getIn() {
    return inputs;
  }

  public ArrayList<Tuple> getOut() {
    return outputs;
  }

  public void addIn(BitTransaction.Tuple p) {
    inputs.add(p);
  }

  public void addOut(BitTransaction.Tuple p) {
    outputs.add(p);
  }

  public String toString() {
    return new StringBuilder()
        .append("transaction:[")
        .append(id).append(",")
        .append(new java.util.Date(lockTime).toString())
        .append("]")
        .toString();
  }

}
