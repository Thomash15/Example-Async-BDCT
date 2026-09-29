package io.pactflow.example.kafka;

import org.springframework.messaging.Message;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import java.nio.charset.StandardCharsets;

public class ProductMessageBuilder {
  private ObjectMapper mapper = new ObjectMapper();
  private ProductEvent product;
  private String correlationId;

  public ProductMessageBuilder withCorrelationId(String correlationId) {
    this.correlationId = correlationId;
    return this;
  }

  public ProductMessageBuilder withProduct(ProductEvent product) {
    this.product = product;
    return this;
  }

  public Message<String> build() throws JacksonException {
    var builder = MessageBuilder.withPayload(this.mapper.writeValueAsString(this.product))
        .setHeader(KafkaHeaders.TOPIC, "products").setHeader("Content-Type", "application/json; charset=utf-8");
    if (correlationId != null) {
      // Raw UTF-8 bytes survive Kafka header mapping without JSON string quotes.
      builder.setHeader("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8));
    }
    return builder.build();
  }

}
