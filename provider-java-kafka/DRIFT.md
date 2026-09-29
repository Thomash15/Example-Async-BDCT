# Drift verification for the Java Kafka provider

Drift subscribes to `products`, triggers `POST /products`, captures the correlated
Kafka event and validates it against `asyncapi.json` and the expected payload.
The existing `ProductAsyncApiTest` remains a fast, broker-free schema check.

The provider uses `spring-boot-starter-kafka` to supply Kafka auto-configuration
under Spring Boot 4. The payload `$ref` includes a description because Drift's
bundled AsyncAPI plugin otherwise emits `description: null` in the resolved
schema, which its JSON validator rejects. No payload constraints were relaxed.

## Run locally

Requirements: Java compatible with the existing Gradle build, Docker Compose v2,
Node/npm, Python 3 and Drift authentication. The runner pins Drift to `2608.3.0`.
If Drift requests authentication, run `npx --yes @pactflow/drift@2608.3.0 auth --help`
and use the login flow supported by your account. PactFlow publication additionally
requires the Pact CLI's `pactflow` command and a read/write workspace token.

From `provider-java-kafka`:

```bash
make drift-up
SEND_TEST_EVENTS=false ./gradlew bootRun
```

Wait until Spring reports startup, then in a second terminal in the same folder:

```bash
make drift-test
```

The dedicated Compose project `pact-drift` uses a disposable Kafka broker on
`localhost:9092`. Do not start the older `kafka-cluster.yml` broker at the same
time. `make drift-up` waits for broker readiness and explicitly creates `products`.
The Kafka plugin uses plaintext connections; use local/test infrastructure.

Each run creates a unique `build/drift/run.*` directory containing the exact tested
contract, rendered test case, result bundle and exit code. It also creates a fresh
correlation ID, passed as an optional HTTP header and forwarded as raw UTF-8 Kafka
header bytes. Requests without this header and the event JSON retain their existing
behavior. The AsyncAPI correlation location does not make the header required.

Optional environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DRIFT_PROVIDER_URL` | `http://localhost:8081` | Trigger endpoint base URL |
| `DRIFT_KAFKA_BROKERS` | `localhost:9092` | Kafka address in the run's contract copy |
| `DRIFT_BIN` | npm pinned CLI | Absolute path to an already installed Drift executable |

If you change the broker address, also configure Spring's
`SPRING_KAFKA_BOOTSTRAP_SERVERS` to use the same broker. Run through the script or
Make target: `drift.yaml` is rendered with a fresh correlation ID and its trigger
is copied into the run directory.

Stop the provider with Ctrl-C and remove this test broker with `make drift-down`.

## Publish to PactFlow

Set `PACT_BROKER_BASE_URL`, `PACT_BROKER_TOKEN`, `GIT_COMMIT` and `GIT_BRANCH` in
your shell or CI secrets/environment. Never put tokens into source files.
`GIT_COMMIT` must identify the code actually running: use the commit SHA in CI,
or an explicit unique demo version when testing uncommitted local changes.

```bash
make drift-publish
```

This runs a fresh verification and publishes its AsyncAPI snapshot and Drift
bundle under `pactflow-example-provider-java-kafka`. Failed verifications are
published with their actual failure code; the command then exits nonzero.
If no bundle was generated, publication is refused. Existing result directories
are never reused. Ordinary `make drift-test` does not publish provider contracts.

In CI: start Kafka, create the topic, start the provider, wait for readiness, then
run `make drift-publish`. Configure Drift authentication for the CI account too.
Retain `build/drift/` as artifacts even on failure, and stop the provider/broker
in the job's cleanup step. Gate deployment after successful publication:

```bash
pact-broker can-i-deploy \
  --pacticipant pactflow-example-provider-java-kafka \
  --version "$GIT_COMMIT" --to-environment production
```

Consumer contracts must already be published. Record deployment only after a real
deployment succeeds. No publication or deployment is performed by this setup.

## Demonstrate a failure safely

Change the expected `name` in `drift.yaml` from `Test product` to another value and
run `make drift-test`; restore it and rerun to demonstrate recovery. This changes
only a test expectation. Renaming `name` to `productName` on the Java model is a
separate schema-drift demonstration and requires restarting the application.

## Restore the state before this integration

A verified snapshot is stored outside `build`, so `gradlew clean` cannot remove it:

`../recovery/before-drift-20260923-154250/`

From the repository root, preview the targeted rollback:

```bash
python3 recovery/before-drift-20260923-154250/restore.py
```

Apply it:

```bash
python3 recovery/before-drift-20260923-154250/restore.py --apply
```

It restores only files changed for Drift and removes only the files added for
Drift. It refuses if those files have later edits, preserving your subsequent work.
It leaves the explicitly requested restoration of `ProductEvent.java` in place.
The original on-disk snapshot records that file as missing; the restored Java
file is saved separately in the recovery directory too.

The archive includes tracked and untracked non-ignored files, with SHA-256 checks,
the original Git HEAD, staged/unstaged patches, and a status listing. It preserves
the unrelated edits and deletions that existed before this task. Generated/ignored
build files and caches are excluded. Keep `recovery/` local; it may include personal
untracked files. Do not use `git reset --hard` or `git clean` for this rollback.

## Verification performed

- Six focused Java tests passed (schema validation and Kafka header mapping).
- The real HTTP → Kafka → Drift flow passed with Drift `2608.3.0`.
- A separate schema copy requiring `productName` failed on the real captured event.
- Publishing-script success, failure and missing-bundle paths were checked with
  local CLI stand-ins. No contracts were published to PactFlow during setup.
- Rollback was exercised in a temporary copy, including protection for later edits.

## References

- [Kafka plugin and correlation](https://support.smartbear.com/swagger/contract-testing/docs/en/drift/plugins/kafka-plugin.html)
- [AsyncAPI plugin](https://support.smartbear.com/swagger/contract-testing/docs/en/drift/plugins/async-api.html)
- [Publishing Drift results](https://support.smartbear.com/swagger/contract-testing/docs/en/drift/how-to-guides/publishing-drift-results-to-pactflow.html)
