package app;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.KafkaConfiguration;
import kafka.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.*;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Arrays;

@ComponentScan({"kafka","app"})
@SpringBootApplication
public class App {

  @Autowired
  private ApplicationContext applicationContext;
  private int last_index=0;

  public App() {
  }

  @PostConstruct
  public void init() {
    KafkaConfiguration configuration = applicationContext.getBean(KafkaConfiguration.class);
    Producer producer = applicationContext.getBean(Producer.class);

    producer.setTPS(15);
    producer.start();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
        while(true) {
          new BitcoinsDataFiles("./")
              .loadFiles()
              .stream().sorted()
              .filter(name-> {
                int index=Integer.parseInt(name.substring(3,name.lastIndexOf(".")));
                if (index > last_index ) { last_index=index; return true; }
                else return false;
              }).forEach(producer::add);
          Thread.sleep(1000);
        }
    });

  }

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(App.class);
    app.setDefaultProperties(Collections
        .singletonMap("server.port", "8082"));
    ApplicationContext ctx = app.run(args);
  }

}
