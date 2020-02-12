package app;

import kafka.KafkaConfiguration;
import kafka.Producer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/bitkapi")
public class RemoteAPI {

  private KafkaConfiguration configuration;

  public RemoteAPI() {
  }

  @GetMapping("/bit-time-interval")
  public ResponseEntity<String> message(@RequestBody long start, long end) {
    return null;
  }
}