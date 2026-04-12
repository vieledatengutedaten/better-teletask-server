"""
Tests for scheduler/queues.py — AsyncJobQueue and QueueManager
"""

import pytest
from app.scheduler.queues import AsyncJobQueue, QueueManager
from lib.models.dataclasses import (
    TranscriptionJob,
    TranscriptionParams,
    TranslationJob,
    TranslationParams,
)


def make_transcription(teletask_id: int = 1) -> TranscriptionJob:
    return TranscriptionJob(
        params=TranscriptionParams(teletask_id=teletask_id),
    )


def make_translation(
    teletask_id: int = 1, from_lang: str = "en", to_lang: str = "de"
) -> TranslationJob:
    return TranslationJob(
        params=TranslationParams(
            teletask_id=teletask_id, from_language=from_lang, to_language=to_lang
        ),
    )


class TestAsyncJobQueue:
    @pytest.fixture
    def queue(self):
        return AsyncJobQueue()

    @pytest.mark.asyncio
    async def test_add_and_get_all(self, queue):
        j1 = make_transcription(1)
        j2 = make_transcription(2)
        await queue.add(j1)
        await queue.add(j2)
        assert [j.id for j in await queue.get_all()] == [j1.id, j2.id]

    @pytest.mark.asyncio
    async def test_add_duplicate_rejected(self, queue):
        j = make_transcription(1)
        assert await queue.add(j) is True
        assert await queue.add(j) is False
        assert await queue.size() == 1

    @pytest.mark.asyncio
    async def test_dequeue_fifo(self, queue):
        j1 = make_transcription(1)
        j2 = make_transcription(2)
        await queue.add(j1)
        await queue.add(j2)
        assert (await queue.dequeue()).id == j1.id
        assert (await queue.dequeue()).id == j2.id
        assert await queue.dequeue() is None

    @pytest.mark.asyncio
    async def test_dequeue_n(self, queue):
        jobs = [make_transcription(i) for i in range(5)]
        for j in jobs:
            await queue.add(j)
        batch = await queue.dequeue_n(3)
        assert len(batch) == 3
        assert await queue.size() == 2

    @pytest.mark.asyncio
    async def test_dequeue_n_more_than_available(self, queue):
        await queue.add(make_transcription(1))
        batch = await queue.dequeue_n(10)
        assert len(batch) == 1
        assert await queue.size() == 0

    @pytest.mark.asyncio
    async def test_remove_by_id(self, queue):
        j1 = make_transcription(1)
        j2 = make_transcription(2)
        await queue.add(j1)
        await queue.add(j2)
        removed = await queue.remove_by_id(j1.id)
        assert removed.id == j1.id
        assert await queue.size() == 1

    @pytest.mark.asyncio
    async def test_remove_by_id_not_found(self, queue):
        assert await queue.remove_by_id("nonexistent") is None

    @pytest.mark.asyncio
    async def test_remove_by_teletask_id(self, queue):
        j1 = make_transcription(42)
        j2 = make_transcription(42)
        j3 = make_transcription(99)
        # Force different IDs since same teletask_id would collide
        j2.id = j2.id + "-extra"
        await queue.add(j1)
        await queue.add(j2)
        await queue.add(j3)
        removed = await queue.remove_by_teletask_id(42)
        assert len(removed) == 2
        assert await queue.size() == 1

    @pytest.mark.asyncio
    async def test_peek(self, queue):
        j = make_transcription(1)
        await queue.add(j)
        assert (await queue.peek()).id == j.id
        assert await queue.size() == 1  # not removed

    @pytest.mark.asyncio
    async def test_peek_empty(self, queue):
        assert await queue.peek() is None

    @pytest.mark.asyncio
    async def test_contains_id(self, queue):
        j = make_transcription(1)
        await queue.add(j)
        assert await queue.contains_id(j.id) is True
        assert await queue.contains_id("nope") is False

    @pytest.mark.asyncio
    async def test_sort_by_custom_key_descending(self):
        queue = AsyncJobQueue(
            sort_key=lambda job: getattr(job.params, "teletask_id", None),
            descending=True,
        )
        j1 = make_transcription(1)
        j2 = make_transcription(9)
        j3 = make_transcription(5)
        await queue.add(j1)
        await queue.add(j2)
        await queue.add(j3)
        assert [j.params.teletask_id for j in await queue.get_all()] == [9, 5, 1]

    @pytest.mark.asyncio
    async def test_add_all_adds_and_sorts_once(self):
        queue = AsyncJobQueue(
            sort_key=lambda job: getattr(job.params, "teletask_id", None),
            descending=True,
        )
        jobs = [make_transcription(2), make_transcription(9), make_transcription(5)]
        added = await queue.add_all(jobs)
        assert added == 3
        assert [j.params.teletask_id for j in await queue.get_all()] == [9, 5, 2]

    @pytest.mark.asyncio
    async def test_add_all_skips_duplicates(self):
        queue = AsyncJobQueue()
        j1 = make_transcription(1)
        j2 = make_transcription(2)
        duplicate = make_transcription(3)
        duplicate.id = j2.id
        added = await queue.add_all([j1, j2, duplicate])
        assert added == 2
        assert [j.id for j in await queue.get_all()] == [j1.id, j2.id]


class TestQueueManager:
    @pytest.fixture
    def manager(self):
        return QueueManager()

    @pytest.mark.asyncio
    async def test_add_routes_to_correct_category(self, manager):
        t = make_transcription(1)
        tr = make_translation(2)
        await manager.add(t)
        await manager.add(tr)
        assert len(await manager.get_all("whisper")) == 1
        assert len(await manager.get_all("ollama")) == 1

    @pytest.mark.asyncio
    async def test_priority_dequeued_first(self, manager):
        normal = make_transcription(1)
        prio = make_transcription(2)
        await manager.add(normal, priority=False)
        await manager.add(prio, priority=True)
        jobs = await manager.next("whisper", n=2)
        assert jobs[0].id == prio.id
        assert jobs[1].id == normal.id

    @pytest.mark.asyncio
    async def test_next_respects_n(self, manager):
        for i in range(5):
            await manager.add(make_transcription(i))
        jobs = await manager.next("whisper", n=3)
        assert len(jobs) == 3

    @pytest.mark.asyncio
    async def test_normal_queue_sorted_by_teletask_id_desc(self, manager):
        await manager.add(make_transcription(2), priority=False)
        await manager.add(make_transcription(9), priority=False)
        await manager.add(make_transcription(5), priority=False)
        jobs = await manager.next("whisper", n=3)
        assert [job.params.teletask_id for job in jobs] == [9, 5, 2]

    @pytest.mark.asyncio
    async def test_priority_queue_remains_fifo(self, manager):
        j1 = make_transcription(2)
        j2 = make_transcription(9)
        j3 = make_transcription(5)
        await manager.add(j1, priority=True)
        await manager.add(j2, priority=True)
        await manager.add(j3, priority=True)
        jobs = await manager.next("whisper", n=3)
        assert [job.id for job in jobs] == [j1.id, j2.id, j3.id]

    @pytest.mark.asyncio
    async def test_add_all_groups_by_category(self, manager):
        jobs = [make_transcription(1), make_translation(2), make_transcription(3)]
        added = await manager.add_all(jobs)
        assert added == 3
        assert len(await manager.get_all("whisper")) == 2
        assert len(await manager.get_all("ollama")) == 1

    @pytest.mark.asyncio
    async def test_add_all_normal_queue_uses_desc_sort(self, manager):
        added = await manager.add_all(
            [make_transcription(4), make_transcription(10), make_transcription(7)],
            priority=False,
        )
        assert added == 3
        jobs = await manager.next("whisper", n=3)
        assert [job.params.teletask_id for job in jobs] == [10, 7, 4]

    @pytest.mark.asyncio
    async def test_add_all_priority_queue_remains_fifo(self, manager):
        j1 = make_transcription(4)
        j2 = make_transcription(10)
        j3 = make_transcription(7)
        added = await manager.add_all([j1, j2, j3], priority=True)
        assert added == 3
        jobs = await manager.next("whisper", n=3)
        assert [job.id for job in jobs] == [j1.id, j2.id, j3.id]

    @pytest.mark.asyncio
    async def test_remove_by_id_across_queues(self, manager):
        j = make_transcription(1)
        await manager.add(j, priority=True)
        removed = await manager.remove_by_id(j.id)
        assert removed.id == j.id
        assert len(await manager.get_all("whisper")) == 0

    @pytest.mark.asyncio
    async def test_remove_by_teletask_id_across_categories(self, manager):
        t = make_transcription(42)
        tr = make_translation(42)
        await manager.add(t)
        await manager.add(tr)
        removed = await manager.remove_by_teletask_id(42)
        assert len(removed) == 2
        assert len(await manager.get_all()) == 0

    @pytest.mark.asyncio
    async def test_get_all_no_filter(self, manager):
        await manager.add(make_transcription(1))
        await manager.add(make_translation(2))
        assert len(await manager.get_all()) == 2

    @pytest.mark.asyncio
    async def test_duplicate_rejected(self, manager):
        j = make_transcription(1)
        assert await manager.add(j) is True
        assert await manager.add(j) is False

    @pytest.mark.asyncio
    async def test_wait_for_job_signals(self, manager):
        import asyncio

        async def add_later():
            await asyncio.sleep(0.05)
            await manager.add(make_transcription(1))

        asyncio.create_task(add_later())
        result = await manager.wait_for_job(timeout=1)
        assert result is True

    @pytest.mark.asyncio
    async def test_wait_for_job_timeout(self, manager):
        result = await manager.wait_for_job(timeout=0.05)
        assert result is False
