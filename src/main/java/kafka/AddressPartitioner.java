package kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class AddressPartitioner implements Partitioner {

  private static final int PARTITION_COUNT = 1;

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
      Cluster cluster) {
    return key.toString().hashCode() % PARTITION_COUNT;
  }

  @Override
  public void close() {

  }

}
