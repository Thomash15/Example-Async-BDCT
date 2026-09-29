# Learning outcomes for the current Async BDCT demo

This repository demonstrates an asynchronous Kafka contract-testing flow using a Java consumer, a provider-owned AsyncAPI schema, and PactFlow compatibility checks.

The learning goals below are aligned to the actual demo flow described in `README.md`, `Demo-consumer-side.md`, and `Demo-provider-side.md`.

| Topic | Title | What it covers | Learning objectives |
| --- | --- | --- | --- |
| 1 | Consumer contract generation | The consumer defines the expected Kafka event contract and generates a V4 Pact. | Explain why the consumer owns the contract it expects and how the tested message shape becomes the published artifact. |
| 2 | Provider AsyncAPI contract | The provider describes its public event contract in AsyncAPI. | Understand how the provider documents the message schema and validates real output against that schema. |
| 3 | Local validation and self-verification | The provider checks its own output before publishing. | Learn why provider validation is necessary and why it is not enough by itself to guarantee compatibility. |
| 4 | PactFlow publication | Consumer and provider artifacts are published with version metadata. | Understand how contract artifacts are stored and versioned for compatibility evaluation. |
| 5 | Deployment gate | PactFlow compares versions with `can-i-deploy`. | Learn how the deployment decision is made and why incompatible versions are blocked before release. |
| 6 | Breaking-change demo | The provider changes an event field while the consumer still expects the old shape. | See how self-verification can still pass while compatibility fails and why the gate matters. |
| 7 | Recovery and rollback | The provider restores the original contract and validates again. | Understand how a failing contract can be repaired and how compatibility is restored. |
| 8 | Demo workflow and tooling | Local helper scripts versus manual step-by-step actions. | Know when to use the quick validation scripts and when to follow the manual demo flow for deeper understanding. |

## What learners should understand

By the end of this demo, a learner should be able to:

- explain the difference between the consumer Pact and the provider AsyncAPI contract
- describe what a Kafka event contract represents in this repository
- identify why a provider must validate its real output against the declared schema
- explain why provider self-verification is important but not sufficient on its own
- describe how `can-i-deploy` acts as a deployment gate
- explain what happens during a breaking field rename such as `name` to `productName`
- understand how compatibility is restored after the provider fixes the contract

## Suggested training flow

1. Start with the consumer side and identify what the consumer expects from the product event.
2. Review the generated V4 Pact and the AsyncAPI operation reference used in the message contract.
3. Move to the provider side and confirm how the provider publishes and validates the event payload.
4. Publish both artifacts to PactFlow.
5. Run the compatibility gate using `can-i-deploy`.
6. Introduce the intentional breaking change.
7. Show that provider validation passes while compatibility fails.
8. Restore the original contract and demonstrate a passing gate again.

## Key takeaway

This demo is not just about message schemas. It is about proving that a provider can be internally valid while still being incompatible with the contract the consumer is actually using. The deployment gate is what prevents that mismatch from reaching production.