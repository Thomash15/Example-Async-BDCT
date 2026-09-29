package io.pactflow.example.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ProductAsyncApiTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private JsonSchema schema;
  private String topic;

  @BeforeEach
  void loadSchema() throws Exception {
    // The demo can select a changed provider contract without modifying source files.
    String schemaFile = System.getenv().getOrDefault("BDCT_SCHEMA_FILE", "asyncapi.json");
    JsonNode document = mapper.readTree(Path.of(schemaFile).toFile());
    assertEquals("send", document.at("/operations/sendProductEvent/action").asText());
    assertEquals("#/channels/products",
        document.at("/operations/sendProductEvent/channel/$ref").asText());
    assertEquals("#/components/schemas/ProductEvent",
        document.at("/components/messages/ProductEvent/payload/$ref").asText());
    topic = document.at("/channels/products/address").asText();
    JsonNode payloadSchema = document.at("/components/schemas/ProductEvent");
    assertFalse(payloadSchema.isMissingNode());
    schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
        .getSchema(payloadSchema);
  }

  @ParameterizedTest
  @EnumSource(EventType.class)
  void producedEventMatchesSchema(EventType event) throws Exception {
    var product = new ProductEvent("id1", "product name", "product type",
        "v1", event, 27.0);
    var message = new ProductMessageBuilder().withProduct(product).build();
    var errors = schema.validate(mapper.readTree(message.getPayload()));
    assertTrue(errors.isEmpty(), () -> "Schema violations: " + errors);
    assertEquals(topic, message.getHeaders().get(KafkaHeaders.TOPIC));
    assertEquals("application/json; charset=utf-8",
        message.getHeaders().get("Content-Type"));
  }

  @Test
  void validatorRejectsMissingId() throws Exception {
    var product = new ProductEvent("id1", "product name", "product type",
        "v1", EventType.CREATED, 27.0);
    var message = new ProductMessageBuilder().withProduct(product).build();
    ObjectNode payload = (ObjectNode) mapper.readTree(message.getPayload());
    payload.remove("id");
    assertFalse(schema.validate(payload).isEmpty(),
        "An event without the required id must be rejected");
  }
}
