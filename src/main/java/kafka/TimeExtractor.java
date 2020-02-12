package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TimeExtractor implements TimestampExtractor {

  public long extract(ConsumerRecord<Object, Object> record, long tm) {
    Transaction t = (Transaction) record.value();
    return t.getTimestamp();
  }

}