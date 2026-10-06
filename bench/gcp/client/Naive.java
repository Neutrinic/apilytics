import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.math.BigDecimal;

/** Naive baseline: walk the offset pages one at a time and parse each with Jackson.
  * usage: Naive <base> <mode: bytes|tree>
  */
public class Naive {
  public static void main(String[] args) throws Exception {
    String base = args[0];
    boolean parse = args[1].equals("tree");
    HttpClient http = HttpClient.newHttpClient();
    ObjectMapper mapper = new ObjectMapper();
    long n = 0, sId = 0, bytes = 0;
    BigDecimal dist = BigDecimal.ZERO, fare = BigDecimal.ZERO;
    long t0 = System.nanoTime();
    for (long offset = 0; ; offset += 5000) {
      HttpRequest req = HttpRequest.newBuilder(URI.create(base + "/offset?offset=" + offset + "&limit=5000")).build();
      HttpResponse<InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
      int rows;
      try (InputStream in = resp.body()) {
        if (parse) {
          JsonNode results = mapper.readTree(in).get("results");
          rows = results.size();
          for (JsonNode r : results) {
            n++;
            sId += r.get("benchmark_id").asLong();
            dist = dist.add(r.get("trip_distance").decimalValue());
            fare = fare.add(r.get("fare_amount").decimalValue());
          }
        } else {
          byte[] b = in.readAllBytes();
          bytes += b.length;
          rows = b.length > 20 ? 5000 : 0;
        }
      }
      if (rows == 0) break;
    }
    double secs = (System.nanoTime() - t0) / 1e9;
    System.out.printf("%s secs=%.1f n=%d s_id=%d dist=%s fare=%s bytes=%d%n", args[1], secs, n, sId, dist, fare, bytes);
  }
}
