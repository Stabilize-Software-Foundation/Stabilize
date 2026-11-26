"""A worker that commits one undelayed successor handles it itself, and hands it back when it cannot."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from stabilize.persistence.committed_marks import allow_continuation, take_claimed
from stabilize.queue.messages import get_message_type_name
from stabilize.queue.processor.poll_loop import release

if TYPE_CHECKING:
    from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)


def run_chain(processor: Any, message: Message) -> None:
    """Handle *message*, then each successor its delivery claimed, up to config.continuation_hops in a row."""
    hops = max(0, int(getattr(processor.config, "continuation_hops", 0) or 0))
    key = getattr(processor.queue, "wake_key", None)
    queue_key = key() if callable(key) else None
    current: Message | None = message
    try:
        while current is not None:
            allowed = hops if queue_key is not None and not processor._stopping else 0
            allow_continuation(allowed, queue_key)
            clean = False
            try:
                clean = bool(processor._deliver(current))
            finally:
                claimed: Message | None = take_claimed()
                if claimed is not None and not _may_continue(processor, clean):
                    hand_back(processor.queue, claimed)
                    claimed = None
            hops -= 1
            current = claimed
    finally:
        allow_continuation(0, None)


def _may_continue(processor: Any, clean: bool) -> bool:
    return bool(clean) and not processor._stopping and processor._fault is None


def hand_back(queue: Any, message: Message) -> None:
    try:
        release(queue, message)
    except Exception as error:
        logger.warning(
            "A claimed %s could not be handed back and stays leased until its lock expires: %s",
            get_message_type_name(message),
            error,
        )
