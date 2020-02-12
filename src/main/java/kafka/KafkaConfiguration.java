package kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.time.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaConfiguration {
    public static String KAFKA_BROKERS = "localhost:9092";
    public static String CLIENT_ID="client1";
    public static String TOPIC_NAME="test10";

    private KafkaStreams streams;

    public KafkaConfiguration() {

    }

    public KafkaStreams getStreams() {
        return streams;
    }

    @PostConstruct
    public void createTopology() {

        class DailyWindows extends Windows<TimeWindow> {

            @Override
            public Map<Long, TimeWindow> windowsFor(final long timestamp) {

                final Instant instant = Instant.ofEpochMilli(timestamp);
                LocalDate now = LocalDate.ofInstant(instant, ZoneId.of("UTC"));
                Instant start = now.atTime( 0,0,0 ).toInstant(ZoneOffset.UTC) ;
                Instant end = now.atTime( 23,59,0).toInstant(ZoneOffset.UTC)  ;

                final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
                windows.put(start.toEpochMilli(), new TimeWindow(start.toEpochMilli(),end.toEpochMilli()));

                return windows;
            }
            @Override
            public long size() {
                return Duration.ofDays(1).toMillis(); }
            @Override
            public long gracePeriodMs() { return 0; }
        }

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Transaction> input = streamsBuilder.stream(TOPIC_NAME);

        KTable<Long, Long> stats=
                    input.groupByKey()
                        .windowedBy(new DailyWindows())
                        .aggregate(()->0L,
                                (k, v, agg) -> {
                                    return v.isEmpty()?0:(agg+v.getSum());
                                },
                                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("bitcoin-aggregates-address")
                                        .withValueSerde(new Serdes.LongSerde()))
                        .toStream()
                        .groupBy((k,v)->k.window().start(),Serialized.with(
                                Serdes.Long(), /* key (note: type was modified) */
                                Serdes.Long()))
                        .reduce((v1,v2)->v1+v2,Materialized.as("bitcoin-aggregates-allday"));

        Topology topology=streamsBuilder.build();
        streams = new KafkaStreams(topology, kStreamsConfigs());
       // streams.cleanUp();
        streams.start();

    }

    public boolean isStoreAvailable(final String storeName)
            throws InterruptedException {
        try {
            for (int i = 0; i < 10; i++) {
                if (streams.state() == KafkaStreams.State.RUNNING)
                    break;
                Thread.sleep(1000);
            }
            final ReadOnlyKeyValueStore<Long, Long> store =
                    streams.store(storeName, QueryableStoreTypes.keyValueStore());
            return true;
        } catch (InvalidStateStoreException ignored) {
            ignored.printStackTrace();
        }
        return false;
    }


    public void close() {
        streams.close();
    }

    public static Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CsvSerde.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AddressPartitioner.class.getName());
        return props;
    }

    public static Properties kStreamsConfigs() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bitcoins-input");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CsvSerde.class);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimeExtractor.class);

        return props;
    }


    @Bean
    public  Producer createProducer() {
        return new Producer();
    }

}