package kafka;


import bitcoin.BitTransaction;
import bitcoin.Block;
import bitcoin.BlockReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.*;

public class Producer {

  private static final Logger log = LoggerFactory.getLogger(Producer.class);
  private ExecutorService executorService;
  private KafkaProducer<String, Transaction> template;
  private CountDownLatch latch = null;
  private boolean isStart = false;
  BlockingQueue<String> bQueue = new LinkedBlockingDeque<>(10);
  long last_nanotime=0;
  long tps_int = 0;

  public Producer() {
    template = new KafkaProducer<>(KafkaConfiguration.producerConfigs());
  }

  public void start() {
    isStart = true;
    latch = new CountDownLatch(1);
    executorService = Executors.newSingleThreadExecutor();
    executorService.submit(this::run);
  }

  public void awaitTermination() {
    if (latch != null) {
      try {
        latch.await();
      } catch (InterruptedException e) {
      }
    }
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  public void stop() {
    isStart = false;
  }


  public void add(String fname) {
    try {
      bQueue.put(fname);
    } catch (InterruptedException e) {
    }
  }

  public void setTPS(int tps) {
    if (tps > 0) tps_int=1000000000L/tps;
  }

  private void processBlock(String fname) {
    try {
      Block block;
      log.info("Start parsing block:" + fname);
      BlockReader reader = new BlockReader(new FileInputStream(fname));
      while ((block = reader.next()) != null) {
        ArrayList<BitTransaction> list = block.getTransactions();
        for (BitTransaction t : list) {
          ArrayList<BitTransaction.Tuple> ins = t.getIn();
          for (BitTransaction.Tuple p : ins) {
            if (!p.isCoinBase && (p.address != null)) {
              send(new Transaction(p.address)
                  .setIsEmpty(true)
                  .setTimestamp(t.getLockTime()));
            }
          }
          ArrayList<BitTransaction.Tuple> outs = t.getOut();
          for (BitTransaction.Tuple p : outs) {
              if (p.address == null) {
                  continue;
              }

              if (last_nanotime >0) {
                long delta = System.nanoTime() - last_nanotime;
                if  (delta<tps_int) sleepNanos(tps_int-delta);
              }

              send(new Transaction(p.address)
                .setSum(p.value)
                .setTimestamp(t.getLockTime()));

              last_nanotime=System.nanoTime();
          }
        }
      }
      log.info("End parsing block:" + fname);
    } catch (IOException e) {

    }
  }

  public static void sleepNanos(long sleepFor)
  {
    boolean wasInterrupted = false;
    try {
      long remainTime = TimeUnit.NANOSECONDS.toNanos(sleepFor);
      long end = System.nanoTime() + remainTime;
      while (true)
      {
        try {
          NANOSECONDS.sleep(remainTime);
          return;
        } catch (InterruptedException e) {
          wasInterrupted = true;
          remainTime = end - System.nanoTime();
        }
      }
    } finally {
      if (wasInterrupted) Thread.currentThread().interrupt();
    }
  }

  public void send(Transaction transaction) {
    ProducerRecord<String, Transaction> record = new ProducerRecord<>(
        KafkaConfiguration.TOPIC_NAME, transaction.address, transaction);
    log.info(
        "sent:" + transaction.address + "," + transaction.getSum() + "," + transaction.isEmpty()
            + "," + new Date(transaction.getTimestamp()));
    try {
      RecordMetadata metadata = template.send(record).get();
    } catch (ExecutionException | InterruptedException e) {

    }
  }


  private void run() {
    try {
      while (isStart) {
        String fname = bQueue.take();
        processBlock(fname);
      }
    } catch (InterruptedException e) {
    }
    latch.countDown();
  }
}