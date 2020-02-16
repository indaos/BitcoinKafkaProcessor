package app;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import kafka.KafkaConfiguration;
import kafka.Producer;
import kafka.Queries;
import kafka.Queries.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.MediaType;

@RestController
@RequestMapping("/bitcapi")
public class RemoteAPI {

  @Autowired
  private KafkaConfiguration configuration;
  private Queries query;

  public RemoteAPI() {
  }

  @PostConstruct
  public void initQueries() {
    query = new Queries(configuration.getStreams());
  }

  @RequestMapping(value = "/aggr-by-days", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<Item>> getDays(Optional<Long> start, Optional<Long> end) {
    List<Item> result = query.streamAllDays()
        .filter(i -> (start.orElse(0L) < i.key && i.key <= end.orElse(System.currentTimeMillis())))
        .collect(Collectors.toList());
    ArrayList<Item> result2 = new ArrayList<>();
    if (result.size()>0) {
      result2.add(result.get(result.size() - 1));
    }
    return ResponseEntity.ok(result2);
  }
}