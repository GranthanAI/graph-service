# P1: graph-service, temporary chats never enter the graph

Not pushed.
- G1: `conversation.created` with `payload.temporary == true` (or the legacy flat `temporary`) is skipped: nothing created, INFO `temporary_conversation_skipped` with the `conversation_id`; the consumer still marks the event processed and commits.
- G2: `chat.message.created` with `temporary: true` at the top level (flat) or in `payload` (enveloped) is skipped the same way.
- G3: `conversation.deleted` for an id that is not in the graph stays a no-op (test added).
- G4: `temporary` and `edited_from_message_id` added to the event schemas; `edited_from_message_id` is stored as a property on the Conversation node when present (only then, so older events behave as before).

Tests: new `tests/test_temporary_chats.py` 8 passed (fake repository). Existing suite (without `test_performance.py`): 36 passed, 2 failed (`test_consistency::test_updates_and_soft_delete_consistency`, `test_sync::test_kafka_neo4j_sync`), which also fail on untouched HEAD (they need a live Kafka consumer group). The Neo4j property write in G4 is not exercised against a real Neo4j.
