package kafka;


import bitcoin.BitTransaction;
import bitcoin.Block;
import bitcoin.BlockReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private CountDownLatch latch=null;
    private boolean isStart=false;
    BlockingQueue<String> bQueue = new LinkedBlockingDeque<>(10);

    public Producer() {
        template=new KafkaProducer<>(KafkaConfiguration.producerConfigs());
    }

    public void start() {
        isStart=true;
        latch = new CountDownLatch(1);
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit( () -> {
            try{
                while(isStart) {
                    String fname = bQueue.take();
                    if (fname!=null) {
                        processBlock(fname);
                    }
                }
            }catch(InterruptedException e) {}
            latch.countDown();
        });
    }

    public void awaitTermination() {
        if (latch != null) {
            try{ latch.await(); }catch (InterruptedException e) {}
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    public void stop() {
        isStart=false;
    }


    public void add(String fname) {
      try{  bQueue.put(fname); }catch(InterruptedException e) {}
    }

    private void processBlock(String fname) {
        try{
            Block block=null;
            log.info("Start parsing block:"+fname);
            BlockReader reader= new BlockReader(new FileInputStream(fname));
            while((block=reader.next())!=null) {
                ArrayList<BitTransaction> list=block.getTransactions();
                for (BitTransaction t: list) {
                    ArrayList<BitTransaction.Tuple> ins=t.getIn();
                    for (BitTransaction.Tuple p : ins) {
                        if (!p.isCoinBase && (p.address!=null)) {
                            send(new Transaction(p.address)
                                    .setIsEmpty(true)
                                    .setTimestamp(t.getLockTime()));
                        }
                    }
                    ArrayList<BitTransaction.Tuple> outs=t.getOut();
                    for(BitTransaction.Tuple p : outs) {
                        if (p.address==null) continue;
                        send(new Transaction(p.address)
                                .setSum(p.value)
                                .setTimestamp(t.getLockTime()));
                    }
                }
            }
            log.info("End parsing block:"+fname);
        }catch(IOException e) {

        }
    }

    public void send(Transaction transaction) {
        ProducerRecord<String,Transaction> record = new ProducerRecord<String,Transaction>(KafkaConfiguration.TOPIC_NAME,transaction.address,transaction);
        log.info("sent:"+transaction.address+","+transaction.getSum()+","+transaction.isEmpty()+","+new Date(transaction.getTimestamp()));
        try {
            RecordMetadata metadata = template.send(record).get();
        } catch (ExecutionException e) {

        } catch (InterruptedException e) {

        }
    }


}