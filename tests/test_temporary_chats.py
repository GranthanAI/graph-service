"""Quick wins: temporary chats never enter the graph, and an edited conversation remembers its source message."""

import logging
from unittest.mock import MagicMock

import pytest

from app.services.graph_service import GraphService

pytestmark = pytest.mark.anyio


def service():
    repo = MagicMock()
    repo.create_conversation_with_parent.return_value = {}
    return GraphService(repo), repo


def created(temporary=None, edited=None):
    payload = {"conversation_id": "c1", "user_id": "u1", "title": "t", "status": "ACTIVE"}
    if temporary is not None:
        payload["temporary"] = temporary
    if edited:
        payload["edited_from_message_id"] = edited
    return {"event_id": "e1", "event_type": "conversation.created", "payload": payload}


async def test_g1_temporary_conversation_created_is_skipped_and_logged(caplog):
    svc, repo = service()
    with caplog.at_level(logging.INFO):
        await svc.process_conversation_created(created(temporary=True))
    repo.create_conversation_with_parent.assert_not_called()
    assert any("temporary_conversation_skipped" in r.getMessage() and r.levelno == logging.INFO for r in caplog.records)
    assert any(getattr(r, "conversation_id", None) == "c1" for r in caplog.records)


async def test_normal_conversation_is_still_created_without_the_new_field_or_with_false():
    for ev in (created(), created(temporary=False)):
        svc, repo = service()
        await svc.process_conversation_created(ev)
        kwargs = repo.create_conversation_with_parent.call_args.kwargs
        assert kwargs["conversation_id"] == "c1" and "edited_from_message_id" not in kwargs


async def test_g4_edited_from_message_id_reaches_the_repository():
    svc, repo = service()
    await svc.process_conversation_created(created(edited="m-77"))
    assert repo.create_conversation_with_parent.call_args.kwargs["edited_from_message_id"] == "m-77"


async def test_legacy_flat_conversation_created_with_temporary_is_skipped_too():
    svc, repo = service()
    await svc.process_conversation_created({"conversation_id": "c1", "user_id": "u", "temporary": True})
    repo.create_conversation_with_parent.assert_not_called()


FLAT = {"message_id": "m1", "conversation_id": "c1", "user_id": "u1", "content": "hi", "timestamp": "2026-11-22T00:00:00Z"}


async def test_g2_flat_message_with_temporary_is_skipped_and_without_it_is_written():
    svc, repo = service()
    await svc.process_message_created({**FLAT, "temporary": True})
    repo.create_message_node.assert_not_called()
    repo.create_message_relationships.assert_not_called()
    await svc.process_message_created({**FLAT, "temporary": False})
    repo.create_message_node.assert_called_once()
    svc2, repo2 = service()
    await svc2.process_message_created(FLAT)                       # an older producer: no field at all
    repo2.create_message_node.assert_called_once()


async def test_g2_enveloped_message_with_temporary_in_the_payload_is_skipped():
    svc, repo = service()
    env = {"event_id": "e", "event_type": "chat.message.created",
           "payload": {"conversation_id": "c1", "message_id": "m1", "role": "user", "content": "hi", "temporary": True}}
    await svc.process_message_created(env)
    repo.create_message_node.assert_not_called()


async def test_g3_deleting_a_conversation_that_is_not_in_the_graph_is_a_harmless_no_op():
    svc, repo = service()
    repo.soft_delete_conversation.return_value = None
    repo.delete_conversation.return_value = None
    await svc.process_conversation_deleted({"event_id": "e", "event_type": "conversation.deleted",
                                            "payload": {"conversation_id": "never-created", "user_id": "u"}})
    assert repo.soft_delete_conversation.called or repo.delete_conversation.called    # asked, did not raise


async def test_consumer_marks_a_skipped_temporary_event_processed_and_commits():
    from app.workers.consumer_worker import KafkaConsumerWorker
    svc, repo = service()
    worker = KafkaConsumerWorker.__new__(KafkaConsumerWorker)
    worker.graph_service = svc
    svc.is_event_processed = MagicMock(return_value=False)
    svc.mark_event_processed = MagicMock()
    from app.core.config import settings
    msg = MagicMock()
    msg.topic = settings.KAFKA_CONVERSATION_CREATED_TOPIC
    import json
    msg.value = json.dumps(created(temporary=True)).encode()
    msg.offset = 1
    await worker._handle_message(msg)
    repo.create_conversation_with_parent.assert_not_called()
    svc.mark_event_processed.assert_called_once_with("e1")
