package kafka;

import java.time.Instant;
import java.util.Arrays;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import java.util.stream.Stream;
import java.util.ArrayList;

public class Queries {

  private KafkaStreams streams;

  public static class Item<K, V> {

    public K key;
    public V value;

    public Item(K k, V v) {
      key = k;
      value = v;
    }
  }

  public Queries(KafkaStreams streams) {
    this.streams = streams;
  }

  public ArrayList<Item<Long,Long>> queryAllDays(String storeName) {
    final ReadOnlyKeyValueStore<Long, Long> store =
        streams.store(storeName, QueryableStoreTypes.keyValueStore());
    ArrayList<Item<Long,Long>> res = new ArrayList<>();
    KeyValueIterator<Long, Long> iter = store.all();
    while (iter.hasNext()) {
      KeyValue<Long, Long> next = iter.next();
      res.add(new Item<>(next.key, (long)(Math.abs(next.value)/100000000f)));
    }
    return res;
  }

  public Stream<Item<Long,Long>> streamAllDays(){
    ArrayList<Item<Long,Long>> result= queryAllDays("bitcoin-aggregates-allday");
    return result.stream();
  }

  public ArrayList<Item> queryAddresses(String storeName, long start, long end) {

    final ReadOnlyWindowStore<String, Long> windowStore =
        streams.store(storeName, QueryableStoreTypes.windowStore());

    ArrayList<Item> res = new ArrayList<>();

    KeyValueIterator<Windowed<String>, Long> iter = windowStore.fetchAll(start, end);

    while (iter.hasNext()) {
      KeyValue<Windowed<String>, Long> next = iter.next();

      final Instant instant1 = Instant.ofEpochMilli(next.key.window().start());
      final Instant instant2 = Instant.ofEpochMilli(next.key.window().end());

      res.add(new Item<>(next.key.window().start(), next.value));
    }

    return res;

  }
}
