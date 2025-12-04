"""Common BFS step logic for weaver interfaces."""

import asyncio
import logging
from typing import TypeVar, Callable, Awaitable, Tuple, Any

P = TypeVar('P')  # Parent entity type
C = TypeVar('C')  # Child entity type


async def bfs_cached_step(
    parent: P,
    # Parent info operations
    load_parent_info: Callable[[P], Awaitable[Tuple[P, Any]]],
    save_parent_info: Callable[[P, Any], Awaitable[None]],
    cache_get_parent_info: Callable[[P], Awaitable[Tuple[P, Any]]],
    cache_set_parent_info: Callable[[P, Any], Awaitable[None]],
    # Pending children operations
    load_pending_children_from_parent: Callable[[P], Awaitable[list[C] | None]],
    cache_get_pending_children: Callable[[P], Awaitable[list[C] | None]],
    cache_add_pending_children: Callable[[P, list[C]], Awaitable[None]],
    # Child info operations
    load_child_info: Callable[[C], Awaitable[Tuple[C, Any]]],
    save_child_info: Callable[[C, Any], Awaitable[None]],
    cache_get_child_info: Callable[[C], Awaitable[Tuple[C, Any]]],
    cache_set_child_info: Callable[[C, Any], Awaitable[None]],
    # Link operations
    save_link: Callable[[P, C], Awaitable[None]],
    is_link_committed: Callable[[P, C], Awaitable[bool]],
    commit_link: Callable[[P, C], Awaitable[None]],
    # Logger
    logger: logging.Logger,
) -> Tuple[int, int] | None:
    """
    Common BFS step logic for processing parent to children relationships.

    Args:
        parent: The parent entity to process
        load_parent_info: Load parent info from data source
        save_parent_info: Save parent info to destination
        cache_get_parent_info: Get parent info from cache
        cache_set_parent_info: Set parent info in cache
        load_pending_children_from_parent: Load pending children from parent via data source
        cache_get_pending_children: Get pending children from cache
        cache_add_pending_children: Add pending children to cache
        load_child_info: Load child info from data source
        save_child_info: Save child info to destination
        cache_get_child_info: Get child info from cache
        cache_set_child_info: Set child info in cache
        save_link: Save link to destination
        is_link_committed: Check if link is already committed
        commit_link: Mark link as committed in cache
        logger: Logger instance for logging progress

    Returns:
        Tuple of (n_new_children, n_failed_children) or None if parent processing failed.
    """
    # Step 1: Fetch and save parent info
    parent, parent_info = await cache_get_parent_info(parent)
    if parent_info is None:
        logger.info(f"[Parent] Cache miss, fetching info: {parent}")
        parent, parent_info = await load_parent_info(parent)
        if parent_info is None:
            logger.warning(f"[Parent] Failed to fetch info: {parent}")
            return None
        await save_parent_info(parent, parent_info)
        await cache_set_parent_info(parent, parent_info)
        logger.debug(f"[Parent] Fetched and cached info: {parent}")
    else:
        logger.debug(f"[Parent] Cache hit: {parent}")

    # Step 2: Get or fetch pending children
    children = await cache_get_pending_children(parent)
    if children is None:
        logger.info(f"[Children] Cache miss, fetching children for parent: {parent}")
        children = await load_pending_children_from_parent(parent)
        if children is None:
            logger.warning(f"[Children] Failed to fetch children for parent: {parent}")
            return None
        await cache_add_pending_children(parent, children)
        logger.info(f"[Children] Fetched {len(children)} children for parent: {parent}")
    else:
        logger.debug(f"[Children] Cache hit, {len(children)} children for parent: {parent}")

    # Step 3: Process each child
    async def process_child(child: C):
        n_new_child, n_new_link = 0, 0
        child, child_info = await cache_get_child_info(child)
        if child_info is None:
            logger.info(f"[Child] Cache miss, fetching info: {child}")
            child, child_info = await load_child_info(child)
            if child_info is None:
                logger.warning(f"[Child] Failed to fetch info: {child}")
                return None
            await save_child_info(child, child_info)
            await cache_set_child_info(child, child_info)
            logger.debug(f"[Child] Fetched and cached info: {child}")
            n_new_child = 1
        else:
            logger.debug(f"[Child] Cache hit: {child}")

        # Step 4: Commit link if not already committed
        if not await is_link_committed(parent, child):
            await save_link(parent, child)
            await commit_link(parent, child)
            logger.info(f"[Link] Committed: {parent} -> {child}")
            n_new_link = 1
        else:
            logger.debug(f"[Link] Already committed: {parent} -> {child}")

        return n_new_child, n_new_link

    results = await asyncio.gather(*[process_child(child) for child in children])
    n_new_child = sum([r[0] for r in results if r is not None])
    n_new_link = sum([r[1] for r in results if r is not None])
    n_failed = sum([1 for r in results if r is None])

    logger.info(f"[Summary] Parent {parent}: {n_new_child} new children, {n_new_link} new links, {n_failed} failed")

    return n_new_child, n_new_link, n_failed
