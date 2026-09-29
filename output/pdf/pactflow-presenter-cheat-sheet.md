# Customer demo - presenter cheat sheet
**Story:** provider tests pass, consumer compatibility fails, deployment is blocked.

## Before screen sharing
Use the full guide (pp. 3, 6-8) to prepare the same terminal: variables, pact_cli, publish_candidate, deploy_demo, a verified baseline/demo environment, and backups (DEMO_BACKUP / EVENT_FILE). Start with the original name payload. Keep the consumer version fixed. These helpers must already exist.

## 1. Show the tests and passing baseline
```bash
cd "$DEMO_ROOT"
bash bdct-demo.sh baseline
```
**Show:** ProductsPactTestV4.java -> generated V4 Pact -> ProductAsyncApiTest.java + asyncapi.json.
**Say:** "The consumer processes its expected event. The provider independently validates its output against its schema." Expect consumer success + 4 provider tests passed. Point to operationId: sendProductEvent.

## 2. Show publishing and the green gate
```bash
pact_cli pact-broker publish "$PACT_FILE" \
  --consumer-app-version "$CONSUMER_VERSION" --branch bdct-demo
export CANDIDATE_VERSION="$BASELINE_VERSION"
publish_candidate
deploy_demo
```
**Show:** SaaS consumer Pact, provider AsyncAPI and successful results. Expect DEPLOYMENT ALLOWED. This repeats the prepared baseline publication.

## 3. Break the provider contract - keep the consumer unchanged
In ProductEvent.java, add above private String name:
```java
@com.fasterxml.jackson.annotation.JsonProperty("productName")
```
In asyncapi.json, rename only the payload property name, its required entry, and the example payload key to productName. Keep message name and example name metadata unchanged. No global replace.
```bash
npx --yes --package @asyncapi/cli asyncapi validate \
  provider-java-kafka/asyncapi.json
export CANDIDATE_VERSION="${DEMO_RUN}-breaking"
publish_candidate
deploy_demo
```

## 4. Pause here: this is the customer proof
**Show:** provider self-tests PASS; SaaS cross-contract check FAILS because the unchanged consumer example lacks required productName; terminal says DEPLOYMENT BLOCKED. Check the actual mismatch, not an authentication/missing-result error.
**Say:** "The release meets its own schema, but breaks an existing consumer. The gate stops our deployment step."

## 5. Recover and close
```bash
cp "$DEMO_BACKUP/ProductEvent.java" "$EVENT_FILE"
cp "$DEMO_BACKUP/asyncapi.json" "$DEMO_ROOT/provider-java-kafka/asyncapi.json"
export CANDIDATE_VERSION="${DEMO_RUN}-fixed"
publish_candidate
deploy_demo
```
**Expect:** tests PASS, compatibility PASS, DEPLOYMENT ALLOWED.
**Close:** "The same gate blocks an incompatible release and allows a compatible one."

Remember: deployment is simulated; CI must honor can-i-deploy's exit code. broken-provider demonstrates local schema drift, not the SaaS blocking scenario above.
