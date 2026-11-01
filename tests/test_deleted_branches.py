"""A conversation deleted in Conversation Service is soft-deleted in the graph (status DELETED). It must disappear from the
navigation lists (children / ancestors / descendants) instead of being offered and failing with "conversation not found" when
opened. Runs against the Neo4j the other graph tests use."""
import pytest

from app.api.dependencies import get_db, get_repository

ROOT, KEPT, GONE, GRANDKID = "del-test-root", "del-test-kept", "del-test-gone", "del-test-grandchild"
T = "2026-10-30T10:00:00Z"


@pytest.fixture()
def graph():
    repo = get_repository(get_db())
    for cid in (GRANDKID, GONE, KEPT, ROOT):
        repo.delete_conversation(cid)
    repo.create_conversation_with_parent(conversation_id=ROOT, user_id="u-del", title="Root", status="ACTIVE",
                                         created_at=T, updated_at=T, parent_id=None)
    for cid in (KEPT, GONE):
        repo.create_conversation_with_parent(conversation_id=cid, user_id="u-del", title=cid, status="ACTIVE",
                                             created_at=T, updated_at=T, parent_id=ROOT)
    repo.create_conversation_with_parent(conversation_id=GRANDKID, user_id="u-del", title="Grandchild", status="ACTIVE",
                                         created_at=T, updated_at=T, parent_id=GONE)
    yield repo
    for cid in (GRANDKID, GONE, KEPT, ROOT):
        repo.delete_conversation(cid)


def ids(rows):
    return [r["conversation_id"] for r in rows]


def test_a_deleted_branch_is_no_longer_listed_as_a_child_or_descendant(graph):
    assert set(ids(graph.get_children(ROOT))) == {KEPT, GONE}
    graph.soft_delete_conversation(GONE, "2026-10-30T11:00:00Z")
    assert ids(graph.get_children(ROOT)) == [KEPT]
    assert GONE not in ids(graph.get_descendants(ROOT))


def test_a_deleted_ancestor_is_not_offered_but_live_relatives_are(graph):
    graph.soft_delete_conversation(GONE, "2026-10-30T11:00:00Z")
    assert GONE not in ids(graph.get_ancestors(GRANDKID))
    assert ROOT in ids(graph.get_ancestors(GRANDKID))


def test_nothing_else_changes_for_conversations_that_are_not_deleted(graph):
    assert set(ids(graph.get_children(ROOT))) == {KEPT, GONE}
    assert set(ids(graph.get_descendants(ROOT))) == {KEPT, GONE, GRANDKID}
