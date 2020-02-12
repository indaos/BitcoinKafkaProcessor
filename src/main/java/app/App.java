package app;

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

@ComponentScan({"kafka"})
@SpringBootApplication
public class App {

  @Autowired
  private ApplicationContext applicationContext;

  public App() {
  }

  @PostConstruct
  public void init() {
    KafkaConfiguration configuration = applicationContext.getBean(KafkaConfiguration.class);
    Producer producer = applicationContext.getBean(Producer.class);

    producer.start();

    new BitcoinsDataFiles("./")
        .loadFiles()
        .stream().forEach(producer::add);

    producer.awaitTermination();

  }

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(App.class);
    app.setDefaultProperties(Collections
        .singletonMap("server.port", "8082"));
    app.run(args);
  }

}
