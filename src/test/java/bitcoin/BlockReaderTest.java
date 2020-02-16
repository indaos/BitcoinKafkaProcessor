package bitcoin;

import kafka.KafkaConfiguration;
import kafka.Producer;
import kafka.Queries;
import kafka.Queries.Item;
import kafka.Transaction;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.Before;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.kafka.streams.TopologyTestDriver;


public class BlockReaderTest {

  private static final Logger log = LoggerFactory.getLogger(Producer.class);

  private static int MAX_DAYS = 10;
  private static int MAX_TRANS = 3;
  private static int MAX_AMOUNT = 1200;
  private static int MAX_ADDRS = 5;

  private static ArrayList<Transaction> transactions = new ArrayList<>();

  private static HashMap<Long, Long> hash_days;
  private static HashMap<String, Long> hash_addresses;


  @BeforeAll
  public static void createMockTransacations() {

    hash_days = new HashMap<>();
    hash_addresses = new HashMap<>();

    ArrayList<String> addresses = new ArrayList<>();
    for (int j = 0; j < MAX_ADDRS; j++) {
      addresses.add(randomString());
    }

    long currTime = System.currentTimeMillis();

    for (int i = 0; i < MAX_DAYS; i++) {
      long start_time = currTime;
      long day_amount = 0;
      for (String address : addresses) {
        long addr_amount = 0;
        for (int k = 0; k < MAX_TRANS; k++) {
          transactions.add(new Transaction()
              .setTimestamp(currTime)
              .setSum(MAX_AMOUNT)
              .setAddress(address));
          currTime += 1000;
          addr_amount += MAX_AMOUNT;
        }
        hash_addresses.put(address + "_" + start_time, addr_amount);
        day_amount += addr_amount;
      }
      hash_days.put(start_time, day_amount);
      currTime += 24 * 60 * 60 * 1000;
    }

  }

  private static String randomString() {
    StringBuilder sb = new StringBuilder();
    String chars = "qwertyQWERTYUIuiopOPASDFGasdfghjklHJKZXCVBNMzxcvbnm";
    Random r = new Random();
    for (int i = 0; i < 34; i++) {
      sb.append(chars.charAt(r.nextInt(chars.length())));
    }
    return sb.toString();
  }


  @Test
  public void testReadingRealBitcoinBlock() {
    try {
      FileInputStream input = new FileInputStream("./blk00025.dat");
      BlockReader reader = new BlockReader(input);

      Block block;
      int nc = 0, nca = 0;

      if ((block = reader.next()) != null) {
        ArrayList<BitTransaction> list = block.getTransactions();

        assertNotNull(list);
        assertNotEquals(list.size(), 0);

        for (BitTransaction t : list) {

          ArrayList<BitTransaction.Tuple> ins = t.getIn();
          ArrayList<BitTransaction.Tuple> outs = t.getOut();

          for (BitTransaction.Tuple p : ins) {
            System.out.println(p.toString());
          }

          for (BitTransaction.Tuple p : outs) {
            System.out.println(p.toString());
          }

        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  @Test
  public void testStreamingWithRealBroker() {
    try {
      KafkaConfiguration configuration = new KafkaConfiguration();
      configuration.createTopology();

      if (configuration.isStoreAvailable("bitcoin-aggregates-allday")) {

        Producer producer = new Producer();
        for (Transaction transaction : transactions) {
          producer.send(transaction);
        }

        Queries query = new Queries(configuration.getStreams());
        int max_attempts = 5;
        int attempt_no = 0;

        while (attempt_no < max_attempts) {
          try {
            ArrayList<Queries.Item<Long,Long>> alldays = query.queryAllDays("bitcoin-aggregates-allday");

            if (alldays == null || alldays.size() == 0) {
              log.info("the database  bitcoin-aggregates-allday is empty yet ... waiting");
              Thread.sleep(10000);
              log.info("try again attempt=" + attempt_no++);
              continue;
            }
            assertEquals(alldays.size(), MAX_DAYS);

            for (Item item : alldays) {
              assertEquals(item.value,
                  MAX_AMOUNT * MAX_TRANS * MAX_ADDRS);
            }

            long end = System.currentTimeMillis() + 24 * 60 * 60 * 1000 * 11;
            ArrayList<Queries.Item> addresses = query
                .queryAddresses("bitcoin-aggregates-address", 0, end);

            if (addresses == null || addresses.size() == 0) {
              log.info("the database bitcoin-aggregates-address is empty yet ... waiting");
              Thread.sleep(10000);
              log.info("try again attempt=" + attempt_no++);
              continue;
            }

            assertEquals(addresses.size(), MAX_ADDRS * MAX_DAYS);

            for (Item item : addresses) {
              assertEquals(item.value,
                  MAX_AMOUNT * MAX_TRANS);
            }

            break;

          } catch (InterruptedException ignored) {
          }
        }

      }

      configuration.close();

    } catch (InterruptedException ignored) {
    }
  }

}
