package kafka;

import java.time.Instant;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;

import java.util.ArrayList;

public class Queries {

    private KafkaStreams streams;

    public class Item<K,V> {
        public K key;
        public V value;

        Item(K k,V v) {
            key=k;
            value=v;
        }
    }

    public Queries(KafkaStreams streams) {
        this.streams=streams;
    }

    public ArrayList<Item> queryAllDays(String storeName) {
        final ReadOnlyKeyValueStore<Long, Long> store =
                streams.store(storeName, QueryableStoreTypes.keyValueStore());
        ArrayList<Item> res = new ArrayList<>();
        KeyValueIterator<Long,Long> iter=store.all();
        while(iter.hasNext()) {
            KeyValue<Long, Long> next = iter.next();
            res.add(new Item<Long,Long>(next.key,next.value));
        }
        return res;
    }

    public ArrayList<Item>  queryAddresses(String storeName,long  start,long end) {

        final ReadOnlyWindowStore<String, Long> windowStore =
                streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());

        ArrayList<Item> res = new ArrayList<>();

        KeyValueIterator<Windowed<String>,Long> iter = windowStore.fetchAll(start,end);

        while(iter.hasNext()) {
            KeyValue<Windowed<String>, Long> next = iter.next();

            final Instant instant1 = Instant.ofEpochMilli(next.key.window(). start());
            final Instant instant2 = Instant.ofEpochMilli(next.key.window().end());

            res.add(new Item<Long,Long>(next.key.window().start(),next.value));
        }

        return res;

    }
}
