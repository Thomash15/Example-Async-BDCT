# Pact Message workshop

## Introduction

This workshop is aimed at demonstrating core features and benefits of contract testing message based systems with Pact.

Modern distributed architectures are increasingly integrated in a decoupled, asynchronous fashion. Message queues such as ActiveMQ, RabbitMQ, SNS, SQS, Kafka and Kinesis are common, often integrated via small and frequent numbers of microservices (e.g. lambda). These sorts of interactions are referred to as "message pacts".

There are some minor differences between how Pact works in these cases when compared to the HTTP use case. Pact supports messages by abstracting away the protocol and specific queuing technology (such as Kafka) and focusses on the messages passing between them.

When writing tests, Pact takes the place of the intermediary (MQ/broker etc.) and confirms whether or not the consumer is able to _handle_ a given event, or that the provider will be able to _produce_ the correct message.

This workshop should take from 1 to 2 hours, depending on how deep you want to go into each topic.

**Workshop outline**:

- [step 1: **create consumer(subscriber)**](https://github.com/YOU54F/pact-workshop-message?tab=readme-ov-file#step-1---create-consumer-subscriber): Create our consumer before the Provider API even exists
- [step 2: **create consumer pact test**](https://github.com/YOU54F/pact-workshop-message?tab=readme-ov-file#step-2---create-consumer-pact-test): Write a Pact test for our consumer
- [step 3: **create provider(publisher)**](https://github.com/YOU54F/pact-workshop-message?tab=readme-ov-file#step-3---create-provider-publisher): Create our provider
- [step 4: **create provider pact test**](https://github.com/YOU54F/pact-workshop-message?tab=readme-ov-file#step-4---create-provider-pact-test): Write a Pact test to verify our provider codebase
<!-- - [step 5: **extend consumer**](https://github.com/pact-foundation/pact-workshop-js/tree/step5#step-5---back-to-the-client-we-go): Extend our consumer -->

<!-- _NOTE: Each step is tied to, and must be run within, a git branch, allowing you to progress through each stage incrementally._

_EG: Move to step 2:_

_`git checkout step2`_

_`npm install`_ -->

<hr/>

**Workshop flavours**:

The workshop is designed in a variety of languages, in order to showcase Pact's many first class client facing DSL's and also highlight the interoperability of Pact.

- Pact-JS
- Pact-JVM
- Pact-Net
- Pact-Go
- Pact-Python
- Pact-Rust

You can mix and match any of the examples.

There are sub-folders with both a `consumer` and `provider` application, in the format

`<application>-<language>-<queue_type>`

- consumer-dotnet-kafka
- provider-dotnet-kafka
- consumer-rust-kafka
- provider-rust-kafka
- consumer-go-kafka
- provider-go-kafka
- provider-python-kafka
- consumer-python-kafka
- provider-js-kafka
- consumer-js-kafka
- consumer-java-kafka
- provider-java-kafka

## Learning objectives

If running this as a team workshop format, you may want to take a look through the [learning objectives](./LEARNING.md).

## Requirements

1 of the following languages, depending on which examples you want to run.

- Node 16+
- Java 17+
- Python 3.9+
- Rust
- Dotnet 8
- Go 1.21+

To run the kafka queue and applications E2E (not required for Pact testing)

[Docker](https://www.docker.com)
[Docker Compose](https://docs.docker.com/compose/install/)

## Scenario

There are two components in scope for our workshop.

1. Product Catalog API (Consumer/Subscriber). a simple HTTP service that exposes product information as a REST API, which is fed events from an Event API on the `product` topic.
2. Product Event (Provider). A service that publishes product events to a Kafka stream on the `product` topic.
uct.

We will only be focusing on the messages sent via Kafka and not the HTTP service, you can follow the HTTP workshop, to see Pact in action against the HTTP endpoints.

![Scenario Overview](./docs/kafka-react.png)

## Step 1 - Create Consumer (subscriber)

We recommend that you split the code that is responsible for handling the protocol specific things - for example an AWS lambda handler and the AWS SNS input body - and the piece of code that actually handles the payload.

You're probably familiar with layered architectures such as Ports and Adapters (also referred to as a Hexagonal architecture). Following a modular architecture will allow you to do this much more easily:

![Ports and Adapters architecture](./docs/ports-and-adapters.png)

Let's walk through an example using a `product event` published through a message broker, in our instance Kafka as an example.

The consumer expects to receive a message of the following shape:

```json
{
  "id": "some-uuid-1234-5678",
  "type": "spare",
  "name": "3mm hex bolt",
  "version": "v1",
  "event": "UPDATED"
}
```


With this view, the "Adapter" will be the code that deals with the specific queue implementation. For example, it might be the lambda `handler` that receives the SNS message that wraps this payload, or the function that can read the message from a Kafka queue (wrapped in a Kafka specific container). Here is the lambda version:

```js
const handler = async (event) => {
  console.info(event);

  // Read the SNS message and pass the contents to the actual message handler
  const results = event.Records.map((e) => receiveProductUpdate(JSON.parse(e.Sns.Message)));

  return Promise.all(results);
};
```

The "Port" is the code (here `receiveProductUpdate`) that is unaware of the fact it's talking to SNS or Kafka, and only deals in the domain itself - in this case the `product event`.

```js
const receiveProductUpdate = (product) => {
  console.log('received product:', product)

  // do something with the product event, e.g. store in the database
  return repository.insert(new Product(product.id, product.type, product.name, product.version))
}
```

This function is the target of the Pact test on the consumer side, which we will create in step 2

## Step 2 - Create Consumer Pact test


Pact is a consumer-driven contract testing tool, which is a fancy way of saying that the API `Consumer` writes a test to set out its assumptions and needs of its API `Provider`(s). By unit testing our API client with Pact, it will produce a `contract` that we can share to our `Provider` to confirm these assumptions and prevent breaking changes.

The process looks like this on the consumer side:

![diagram](https://raw.githubusercontent.com/pact-foundation/pact-js/master/docs/diagrams/message-consumer.png)

The process looks like this on the provider (producer) side:

![diagram](https://raw.githubusercontent.com/pact-foundation/pact-js/master/docs/diagrams/message-provider.png)

1. The consumer writes a unit test of its behaviour using a Mock provided by Pact.
1. Pact writes the interactions into a contract file (as a JSON document).
1. The consumer publishes the contract to a broker (or shares the file in some other way).
1. Pact retrieves the contracts and replays the requests against a locally running provider.
1. The provider should stub out its dependencies during a Pact test, to ensure tests are fast and more deterministic.

In this section we will look at 1 & 2, writing the unit test which will generate the contract file we can share with our provider.

```js

// 1. The target of our test, our Product Event Handler
const productEventHandler = require('./product.handler')

// 2. Import Pact DSL for your language of choice
const {
  MatchersV3,
  MessageConsumerPact,
  asynchronousBodyHandler,
} = require("@pact-foundation/pact");
const { like, regex } = MatchersV3;

const path = require("path");

describe("Kafka handler", () => {
// 3. Setup Pact Message Consumer Constructor
// specifying consumer & provider naming
// and any required options
  const messagePact = new MessageConsumerPact({
    consumer: "pactflow-example-consumer-js-kafka",
    dir: path.resolve(process.cwd(), "pacts"),
    pactfileWriteMode: "update",
    provider: "pactflow-example-provider-js-kafka",
    logLevel: process.env.PACT_LOG_LEVEL ?? "info",
  });

  describe("receive a product update", () => {
    it("accepts a product event", () => {
    // 4. Arrange - Setup our message expectations
      return messagePact
        // The description for the event
        //   Used in the provider side verification to map to 
        //   a function that will produce this message
        .expectsToReceive("a product event update")
        // The contents of the message, we expect to receive
        //   Pact matchers can be applied, to allow for flexible
        //   verification, based on applied matchers.
        .withContent({
          id: like("some-uuid-1234-5678"),
          type: like("Product Range"),
          name: like("Some Product"),
          version: like("v1"),
          event: regex("^(CREATED|UPDATED|DELETED)$","UPDATED"),
        })
        // Setup any required metadata
        //   A consumer may require additional data, which does not
        //   form part of the message content. This could be any
        //   that can be encoded in a key value pair, that is 
        //   serialisable to json. In our case, it is the kafka
        //   topic our consumer will subscribe to
        .withMetadata({
          "contentType": "application/json",
          "kafka_topic": "products",
        })
        // 5. Act
        //      Pact provides a verification function where the message
        //      content, and metadata are made available, in order to process
        //      and pass to your system under test, our Product Event Handler.
        //
        //      Some Pact DSL's will provide body handlers, as convenience functions
        //       
        .verify(asynchronousBodyHandler(productEventHandler));
    });
  });
});
```


1.  The target of our test, our Product Event Handler.
    - In most applications, some form of transactionality exists and communication with a MQ/broker happens.
    - It's important we separate out the protocol bits from the message handling bits, so that we can test that in isolation.
2.  Import Pact DSL for your language of choice
3.  Setup Pact Message Consumer Constructor, which will vary slightly depending on your implementation. Here you can setup the name of the consumer/provider pair for the test, and any required pact options
4.  Setup the expectations for the consumer 
    1. The description for the event
       1. Used in the provider side verification to map to  a function that will produce this message
    2. The contents of the message, we expect to receive
    3. Pact matchers can be applied, to allow for flexible  verification, based on applied matchers.
    4. Setup any required metadata
       1. A consumer may require additional data, which does not form part of the message content. This could be any that can be encoded in a key value pair, that is serialisable to json. In our case, it is the kafka topic our consumer will subscribe to.
5.  Pact will send the message to your message handler. If the handler returns a successful promise, the message is saved, otherwise the test fails. There are a few key things to consider:
    - The actual request body that Pact will send, will be contained within a [Message](https://github.com/pact-foundation/pact-js/tree/master/src/dsl/message.ts) object along with other context, so the body must be retrieved via `content` attribute.
    - All handlers to be tested must be of the shape `(m: Message) => Promise<any>` - that is, they must accept a `Message` and return a `Promise`. This is how we get around all of the various protocols, and will often require a lightweight adapter function to convert it.
    - In this case, we wrap the actual productEventHandler with a convenience function `asynchronousBodyHandler` provided by Pact, which Promisifies the handler and extracts the contents.

You can now run the test.

```sh
> product-service@1.0.0 test
> jest --testTimeout 30000


 RUNS  src/product/product.handler.pact.test.js
 PASS  src/product/product.handler.pact.test.js
  ● Console

    console.log
      received product: {
        event: 'UPDATED',
        id: 'some-uuid-1234-5678',
        name: 'Some Product',
        type: 'Product Range',
        version: 'v1'
      }

      at log (src/product/product.handler.js:5:11)

    console.log
      received product event: UPDATED

      at log (src/product/product.handler.js:6:11)

 PASS  src/product/product.repository.test.js

Test Suites: 2 passed, 2 total
Tests:       2 passed, 2 total
Snapshots:   0 total
Time:        0.601 s, estimated 1 s
```

Take a look at the pact directory, at the generated contract.

```json
{
  "consumer": {
    "name": "pactflow-example-consumer-js-kafka"
  },
  "messages": [
    {
      "contents": {
        "event": "UPDATED",
        "id": "some-uuid-1234-5678",
        "name": "Some Product",
        "type": "Product Range",
        "version": "v1"
      },
      "description": "a product event update",
      "matchingRules": {
        "body": {
          "$.event": {
            "combine": "AND",
            "matchers": [
              {
                "match": "regex",
                "regex": "^(CREATED|UPDATED|DELETED)$"
              }
            ]
          },
          "$.id": {
            "combine": "AND",
            "matchers": [
              {
                "match": "type"
              }
            ]
          },
          "$.name": {
            "combine": "AND",
            "matchers": [
              {
                "match": "type"
              }
            ]
          },
          "$.type": {
            "combine": "AND",
            "matchers": [
              {
                "match": "type"
              }
            ]
          },
          "$.version": {
            "combine": "AND",
            "matchers": [
              {
                "match": "type"
              }
            ]
          }
        },
        "metadata": {}
      },
      "metadata": {
        "contentType": "application/json",
        "kafka_topic": "products"
      }
    }
  ],
  "metadata": {
    "pact-js": {
      "version": "13.1.4"
    },
    "pactRust": {
      "ffi": "0.4.22",
      "models": "1.2.3"
    },
    "pactSpecification": {
      "version": "3.0.0"
    }
  },
  "provider": {
    "name": "pactflow-example-provider-js-kafka"
  }
}
```

Your handler should throw an error, if it is unable to process the message. Try commenting out a couple of values in the Pact expectations and re-run your test.

```sh
> product-service@1.0.0 test
> jest --testTimeout 30000


 RUNS  src/product/product.handler.pact.test.js
 FAIL  src/product/product.handler.pact.test.jse library successfully found, and the correct version
  ● Console

    console.log
      received product: { id: 'some-uuid-1234-5678' }

      at log (src/product/product.handler.js:5:11)

    console.log
      received product event: undefined

      at log (src/product/product.handler.js:6:11)

  ● Kafka handler › receive a product update › accepts a product event

    Unable to process event

      19 |     );
      20 |   }
    > 21 |   throw new Error("Unable to process event")
         |         ^
      22 | };
      23 |
      24 | module.exports = handler;

      at handler (src/product/product.handler.js:21:9)
      at node_modules/@pact-foundation/src/messageConsumerPact.ts:254:34
      at MessageConsumerPact.Object.<anonymous>.MessageConsumerPact.verify (node_modules/@pact-foundation/src/messageConsumerPact.ts:187:12)
      at Object.verify (src/product/product.handler.pact.test.js:35:10)

 PASS  src/product/product.repository.test.js

Test Suites: 1 failed, 1 passed, 2 total
Tests:       1 failed, 1 passed, 2 total
Snapshots:   0 total
Time:        0.678 s, estimated 1 s
```

Update your test, and re-run it, so your Pact file is up-to-date. We can now move onto step 3, where we will build out our provider code.

## Step 3 - Create Provider (publisher)

For our Provider, we are again going to be following the Ports and Adapters pattern.

We need
    - a "Port" that is responsible for _producing_ the message.
    - an "Adapter" that is responsible for _sending_ the message.

In our case, we have a `ProductEventService` that is responsible for this:

- The `publish` is the bit ("Adapter") that knows how to talk to the message queue
- The `update` is the bit ("Port") that just deals in our domain and knows how to create the specific event structure.
  - `createEvent` This is the function on the provider side that we'll test is able to _produce_ the correct message structure.

```js
class ProductEventService {
  async create(event) {
    const product = productFromJson(event);
    return this.publish(createEvent(product, "CREATED"));
  }

  async update(event) {
    const product = productFromJson(event);
    return this.publish(createEvent(product, "UPDATED"));
  }

  ...
  // Adapter - knows how to 
  async publish(message) {
    const payload = {
      topic: TOPIC,
      messages: [{ value: JSON.stringify(message) }],
    };

    console.log("ProductEventService - sending message:", message);

    return this.producer.send(payload);
  }
}
```

Move onto step 4, where we will create a Pact provider test, which will map our consumer Pact message descriptions
to our `createEvent` function to ensure it will _produce_ the correct message structure.

## Step 4 - Create Provider Pact test

As per the Consumer case, Pact takes the position of the intermediary (MQ/broker) and checks to see whether or not the Provider sends a message that matches the Consumer's expectations.

1.  Our API producer contains a function `createEvent` which is responsible for generating the message that will be sent to the consumer via some message queue. We will use our `Product` domain model as we will use this to ensure the messages we generate comply with our Domain.
2.  Import Pact DSL
3.  We configure Pact to stand-in for the queue. The most important bit here is the `messageProviders` block.
    - Similar to the Consumer tests, we map the various interactions that are going to be verified as denoted by their `description` field. In this case, `a product event update`, maps to the `createEvent` handler. Notice how this matches the original Consumer test. We are using the `providerWithMetadata` function because we are also going to validate message metadata (in this case, the queue the message will be sent on).
4.  We can now run the verification process. Pact will read all of the interactions specified by its consumer, and invoke each function that is responsible for generating that message.

```js
// 1. Import message producing function, and Product domain object
const { createEvent } = require("./product.event");
const { Product } = require("./product");

// 2. Import Pact DSL
const { MessageProviderPact, providerWithMetadata } = require("@pact-foundation/pact");

const path = require("path");

describe("Message provider tests", () => {

  // 3. Arrange
  
  // Pact sources - here we are going to use a local file
  const pactUrl = process.env.PACT_URL || path.join(__dirname, "..", "..", "..", "consumer-js-kafka", "pacts", "pactflow-example-consumer-js-kafka-pactflow-example-provider-js-kafka.json");

  const opts = {
    pactUrls: [pactUrl],
    // Pact message providers
    messageProviders: {
      'a product event update': providerWithMetadata(() => createEvent(new Product("42", "food", "pizza"), "UPDATED"), {
        kafka_topic: 'products',
      }),
    },
  };

  const p = new MessageProviderPact(opts);

  describe("product api publishes an event", () => {
    it("can generate messages for specified consumers", () => {
      // 4. Run the pact verification
      return p.verify();
    });
  });
});
```

We can now run our test

```sh
> product-service@1.0.0 test
> jest --testTimeout 30000 --testMatch "**/*.pact.test.js"


 RUNS  src/product/product.pact.test.js
[21:15:59.007] INFO (36404): pact@13.1.4: Verifying message
[21:15:59.012] INFO (36404): pact-core@15.2.1: Verifying Pacts.
[21:15:59.013] INFO (36404): pact-core@15.2.1: Verifying Pact Files
 RUNS  src/product/product.pact.test.js
2024-10-22T20:15:59.196741Z  INFO ThreadId(11) pact_verifier: Running setup provider state change handler with empty state for 'a product event update'
2024-10-22T20:15:59.196899Z  INFO ThreadId(11) pact_verifier: Running provider verification for 'a product event update'
2024-10-22T20:15:59.196981Z  INFO ThreadId(11) pact_verifier::provider_client: Sending request to provider at http://localhost:58571/
2024-10-22T20:15:59.196984Z  INFO ThreadId(11) pact_verifier::provider_client: Sending request HTTP Request ( method: POST, path: /, query: None, headers: Some({"Content-Type": ["application/json"]}), body: Present(40 bytes, application/json) )
2024-10-22T20:15:59.206234Z  INFO ThreadId(11) pact_verifier::provider_client: Received response: HTTP Response ( status: 200, headers: Some({"date": ["Tue, 22 Oct 2024 20:15:59 GMT"], "connection": ["keep-alive"], "keep-alive": ["timeout=5"], "pact_message_metadata": ["eyJrYWZrYV90b3BpYyI6InByb2R1Y3RzIn0="], "content-length": ["73"], "content-type": ["application/json; charset=utf-8"], "pact-message-metadata": ["eyJrYWZrYV90b3BpYyI6InByb2R1Y3RzIn0="], "x-powered-by": ["Express"], "etag": ["W/\"49-41p5fNWaTSGyF99I4ouOdCtiDE0\""]}), body: Present(73 bytes, application/json;charset=utf-8) )
2024-10-22T20:15:59.207511Z  WARN ThreadId(11) pact_matching::metrics: 

Please note:
We are tracking events anonymously to gather important usage statistics like Pact version and operating system. To disable tracking, set the 'PACT_DO_NOT_TRACK' environment variable to 'true'.

 RUNS  src/product/product.pact.test.js

Verifying a pact between pactflow-example-consumer-js-kafka and pactflow-example-provider-js-kafka

  a product event update (0s loading, 185ms verification)
    generates a message which
      includes metadata
        "contentType" with value "application/json" (OK)
        "kafka_topic" with value "products" (OK)
      has a matching body (OK)

 PASS  src/product/product.pact.test.js
  Message provider tests
    product api publishes an event
      ✓ can generate messages for specified consumers (657 ms)

Test Suites: 1 passed, 1 total
Tests:       1 passed, 1 total
Snapshots:   0 total
Time:        1.233 s
```

Great, the test passed!

Let's take a look at some failing situations.

1. Change the description mapping in the message provider, from `a product event update` to `a product event updated`

```sh
Verifying a pact between pactflow-example-consumer-js-kafka and pactflow-example-provider-js-kafka

  a product event update (4ms loading, 196ms verification)
    generates a message which
      includes metadata
        "contentType" with value "application/json" (OK)
        "kafka_topic" with value "products" (FAILED)
      has a matching body (FAILED)


Failures:

1) Verifying a pact between pactflow-example-consumer-js-kafka and pactflow-example-provider-js-kafka - a product event update
    1.1) has a matching body
           $ -> Actual map is missing the following keys: event, id, name, type, version
    -{
  "event": "UPDATED",
  "id": "some-uuid-1234-5678",
  "name": "Some Product",
  "type": "Product Range",
  "version": "v1"
}
    +{}

    1.2) has matching metadata
           Expected message metadata 'kafka_topic' to have value 'products' but was ''

There were 1 pact failures
 FAIL  src/product/product.pact.test.js
  Message provider tests
    product api publishes an event
      ✕ can generate messages for specified consumers (466 ms)

  ● Message provider tests › product api publishes an event › can generate messages for specified consumers

    Verfication failed

      at node_modules/@pact-foundation/pact-core/src/verifier/nativeVerifier.ts:52:20

Test Suites: 1 failed, 1 total
Tests:       1 failed, 1 total
Snapshots:   0 total
Time:        1.172 s, estimated 2 s
```

Great, we can see a failure, where we don't have a mapping from our message interaction in the consumer pact, in our provider test. Change it back to `a product event update`.

You can expect Pact to fail, where there is no defined handler for a message, which ensures that the provider correctly handles each of these cases.

As a consumer generating contracts, one should work with the provider team, in order to ensure mapping can be agreed upon. There may be the opportunity to reuse existing mappings created by other teams.

2. Change some data in the generated event, in your messageProviders. Lets try changing `UPDATED` to `MODIFIED`, and change the metadata key `kafka_topic` to `topic`

Run the test

```sh
Verifying a pact between pactflow-example-consumer-js-kafka and pactflow-example-provider-js-kafka

  a product event update (4ms loading, 200ms verification)
    generates a message which
      includes metadata
        "contentType" with value "application/json" (OK)
        "kafka_topic" with value "products" (FAILED)
      has a matching body (FAILED)


Failures:

1) Verifying a pact between pactflow-example-consumer-js-kafka and pactflow-example-provider-js-kafka - a product event update
    1.1) has a matching body
           $.event -> Expected 'MODIFIED' to match '^(CREATED|UPDATED|DELETED)$'

    1.2) has matching metadata
           Expected message metadata 'kafka_topic' to have value 'products' but was ''

There were 1 pact failures
 FAIL  src/product/product.pact.test.js
  Message provider tests
    product api publishes an event
      ✕ can generate messages for specified consumers (446 ms)

  ● Message provider tests › product api publishes an event › can generate messages for specified consumers

    Verfication failed
```

Great, the test fails, both on the body content, and the returned metadata. 

Here, Pact matchers restricted the value of `$.event`  to be one of `CREATED` / `UPDATED` or `DELETED`, by way of a regular expression.

Our metadata is also checked, to ensure the correct value is generated.

Try reverting the metadata key `topic` back to `kafka_topic`, but change the topic name to `product`..

Running the test again will return a new error about the metadata, telling us the correct key was returned, but the incorrect value was. This will allow us not only to validate the body contents of our messages, but important data wthat will relate to our transmission protocol (or anything else we deem suitable).

```sh

    1.2) has matching metadata
           Expected message metadata 'kafka_topic' to have value '"products"' but was '"product"'
```

 In our instance, if we were posting to a different queue, that the customer was listening to, it may be a while before anyone realises that messages will never be received. Pact gives you early feedback, long before requiring deploying each application, along side a queue and testing in an integration environment

