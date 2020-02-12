package kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import java.util.stream.Stream;

public class Transaction  implements Serde  {

    String address;
    long sum;
    long timestamp;
    boolean empty=false;

    public Transaction() {

    }

    public Transaction(String address) {
        this.address=address;
    }

    public Transaction setIsEmpty(boolean empty) {
        this.empty=empty;
        return this;
    }


    public Transaction add(long s) {
        this.sum+=s;
        return this;
    }

    public Transaction addTotal() {
        return this;
    }

    public Transaction addTotal(Transaction t) {
        return this;
    }

    public String[] getFields(){
        return Stream.of(address,
                Long.valueOf(sum).toString(),
                Long.valueOf(timestamp).toString(),
                Boolean.valueOf(empty).toString())
                .toArray(size->new String[size]);
    }

    public void setFields(String[] fields){
        setAddress(fields[0]);
        setSum(Long.valueOf(fields[1]));
        setTimestamp(Long.valueOf(fields[2]));
        setIsEmpty(Boolean.valueOf(fields[3]));
    }

    public String getAddress() {
        return address;
    }

    public Transaction setAddress(String address) {
        this.address = address;
        return this;
    }

    public long getSum() {
        return sum;
    }

    public Transaction setSum(long sum) {
        this.sum = sum;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Transaction setTimestamp(long timstamp) {
        this.timestamp = timstamp;
        return this;
    }

    public boolean isEmpty() { return empty; }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return new CsvSerde();
    }

    @Override
    public Deserializer deserializer() {
        return new CsvSerde();
    }

}
