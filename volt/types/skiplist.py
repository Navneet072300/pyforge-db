"""
Skip list — the backing structure for Volt sorted sets (ZSet).

Provides O(log n) insert, delete, rank, and range queries.
Levels are chosen probabilistically with p=0.25, max level 32.

Each node carries:  score (float), member (str), forward[level] pointers.
The head sentinel has score=-inf, member=''.
"""
from __future__ import annotations

import math
import random
from typing import List, Optional, Tuple

_MAX_LEVEL = 32
_P         = 0.25


class _Node:
    __slots__ = ('score', 'member', 'forward', 'span')

    def __init__(self, score: float, member: str, level: int):
        self.score   = score
        self.member  = member
        self.forward: List[Optional['_Node']] = [None] * level
        self.span:    List[int]                = [0]   * level


class SkipList:
    """
    Ordered by (score, member) — ties broken lexicographically by member.
    `span[i]` on each node counts how many nodes are skipped at level i,
    enabling O(log n) rank computation.
    """

    def __init__(self):
        self._head   = _Node(-math.inf, '', _MAX_LEVEL)
        self._level  = 1
        self._length = 0
        self._scores: dict[str, float] = {}   # member → score (fast lookup)

    def __len__(self) -> int:
        return self._length

    # ── mutation ──────────────────────────────────────────────────────────────

    def insert(self, score: float, member: str):
        """Insert or update member with score."""
        if member in self._scores:
            old_score = self._scores[member]
            if old_score == score:
                return
            self.remove(member)

        update: List[Optional[_Node]] = [None] * _MAX_LEVEL
        rank:   List[int]             = [0]   * _MAX_LEVEL

        node = self._head
        for i in range(self._level - 1, -1, -1):
            rank[i] = rank[i + 1] if i < self._level - 1 else 0
            while (node.forward[i] is not None and
                   self._lt(node.forward[i], score, member)):
                rank[i] += node.span[i]
                node = node.forward[i]
            update[i] = node

        lvl = self._random_level()
        if lvl > self._level:
            for i in range(self._level, lvl):
                rank[i]   = 0
                update[i] = self._head
                update[i].span[i] = self._length
            self._level = lvl

        new_node = _Node(score, member, lvl)
        for i in range(lvl):
            new_node.forward[i]    = update[i].forward[i]
            update[i].forward[i]   = new_node
            new_node.span[i]       = update[i].span[i] - (rank[0] - rank[i])
            update[i].span[i]      = (rank[0] - rank[i]) + 1

        for i in range(lvl, self._level):
            update[i].span[i] += 1

        self._length += 1
        self._scores[member] = score

    def remove(self, member: str) -> bool:
        """Remove member. Returns True if it existed."""
        if member not in self._scores:
            return False
        score = self._scores[member]

        update: List[Optional[_Node]] = [None] * _MAX_LEVEL
        node = self._head
        for i in range(self._level - 1, -1, -1):
            while (node.forward[i] is not None and
                   self._lt(node.forward[i], score, member)):
                node = node.forward[i]
            update[i] = node

        target = update[0].forward[0]
        if target is None or target.member != member:
            return False

        for i in range(self._level):
            if update[i].forward[i] is not target:
                update[i].span[i] -= 1
            else:
                update[i].span[i] += target.span[i] - 1
                update[i].forward[i] = target.forward[i]

        while self._level > 1 and self._head.forward[self._level - 1] is None:
            self._level -= 1

        self._length -= 1
        del self._scores[member]
        return True

    # ── queries ───────────────────────────────────────────────────────────────

    def score(self, member: str) -> Optional[float]:
        return self._scores.get(member)

    def rank(self, member: str) -> Optional[int]:
        """0-based rank (ascending). None if not found."""
        if member not in self._scores:
            return None
        target_score = self._scores[member]
        r    = 0
        node = self._head
        for i in range(self._level - 1, -1, -1):
            while (node.forward[i] is not None and
                   self._lt(node.forward[i], target_score, member)):
                r    += node.span[i]
                node  = node.forward[i]
        # node is now the predecessor; the target is node.forward[0]
        fwd = node.forward[0]
        if fwd is not None and fwd.member == member:
            return r
        return None

    def range_by_rank(self, start: int, stop: int
                      ) -> List[Tuple[str, float]]:
        """Inclusive [start, stop], 0-based. Returns [(member, score), ...]."""
        n = self._length
        if start < 0:
            start = max(0, n + start)
        if stop < 0:
            stop = n + stop
        if start > stop or start >= n:
            return []
        stop = min(stop, n - 1)

        results = []
        traversed = 0
        node = self._head
        # Jump to start position
        for i in range(self._level - 1, -1, -1):
            while node.forward[i] and traversed + node.span[i] <= start:
                traversed += node.span[i]
                node = node.forward[i]
        node = node.forward[0]
        while node and traversed <= stop:
            results.append((node.member, node.score))
            node = node.forward[0]
            traversed += 1
        return results

    def range_by_score(self, min_score: float, max_score: float,
                       min_exclusive: bool = False,
                       max_exclusive: bool = False,
                       offset: int = 0, count: int = -1
                       ) -> List[Tuple[str, float]]:
        results = []
        node = self._head
        # Skip past nodes below min_score
        for i in range(self._level - 1, -1, -1):
            while node.forward[i] and (
                node.forward[i].score < min_score or
                (min_exclusive and node.forward[i].score == min_score)
            ):
                node = node.forward[i]
        node = node.forward[0]
        skipped = 0
        while node:
            if max_exclusive and node.score >= max_score:
                break
            if not max_exclusive and node.score > max_score:
                break
            if skipped < offset:
                skipped += 1
            else:
                results.append((node.member, node.score))
                if count != -1 and len(results) >= count:
                    break
            node = node.forward[0]
        return results

    def all_members(self) -> List[Tuple[str, float]]:
        results = []
        node = self._head.forward[0]
        while node:
            results.append((node.member, node.score))
            node = node.forward[0]
        return results

    # ── internals ─────────────────────────────────────────────────────────────

    @staticmethod
    def _lt(node: _Node, score: float, member: str) -> bool:
        return node.score < score or (node.score == score and node.member < member)

    @staticmethod
    def _random_level() -> int:
        level = 1
        while level < _MAX_LEVEL and random.random() < _P:
            level += 1
        return level
