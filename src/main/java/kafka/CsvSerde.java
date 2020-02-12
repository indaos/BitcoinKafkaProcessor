package kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public  class CsvSerde<T extends Serde> implements Serializer<T>, Deserializer<T>, Serde<T> {

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {}

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            String[] fields=new String(data).split(",");
            Transaction bt=new Transaction();
            bt.setAddress(fields[0]);
            bt.setSum(Long.valueOf(fields[1]));
            bt.setTimestamp(Long.valueOf(fields[2]));
            return (T)bt;
        } catch (final NullPointerException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        }
        try {
            String[] fields = ((Transaction)data).getFields();
            StringBuilder sb = new StringBuilder();
            for(int i=0;i<fields.length;i++) {
                sb.append(fields[i]);
                if (i!=fields.length-1)
                    sb.append(",");
            }
            return sb.toString().getBytes();
        } catch (final Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {}

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}