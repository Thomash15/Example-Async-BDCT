package io.pactflow.example.kafka;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.JsonKafkaHeaderMapper;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static org.junit.jupiter.api.Assertions.*;

class ProductCorrelationTest {
  private final ProductEvent product = new ProductEvent(
      "id1", "product name", "BOOK", "v1", EventType.CREATED, 27.0);

  @Test
  void correlationSurvivesKafkaHeaderMappingAsUnquotedUtf8() throws Exception {
    var message = new ProductMessageBuilder().withProduct(product)
        .withCorrelationId("drift-run-123").build();
    var headers = new RecordHeaders();
    new JsonKafkaHeaderMapper().fromHeaders(message.getHeaders(), headers);
    assertArrayEquals("drift-run-123".getBytes(StandardCharsets.UTF_8),
        headers.lastHeader("correlation-id").value());
  }

  @Test
  void existingProducersNeedNoCorrelationHeader() throws Exception {
    var original = new ProductMessageBuilder().withProduct(product).build();
    var correlated = new ProductMessageBuilder().withProduct(product)
        .withCorrelationId("drift-run-123").build();
    assertNull(original.getHeaders().get("correlation-id"));
    assertEquals(original.getPayload(), correlated.getPayload());
  }
}
