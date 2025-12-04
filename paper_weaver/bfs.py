"""Common BFS step logic for weaver interfaces."""

import asyncio
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

    Returns:
        Tuple of (n_new_children, n_failed_children) or None if parent processing failed.
    """
    # Step 1: Fetch and save parent info
    parent, parent_info = await cache_get_parent_info(parent)
    if parent_info is None:
        parent, parent_info = await load_parent_info(parent)
        if parent_info is None:
            return None
        await save_parent_info(parent, parent_info)
        await cache_set_parent_info(parent, parent_info)

    # Step 2: Get or fetch pending children
    children = await cache_get_pending_children(parent)
    if children is None:
        children = await load_pending_children_from_parent(parent)
        if children is None:
            return None
        await cache_add_pending_children(parent, children)

    # Step 3: Process each child
    async def process_child(child: C):
        n_new = 0
        child, child_info = await cache_get_child_info(child)
        if child_info is None:
            child, child_info = await load_child_info(child)
            if child_info is None:
                return None
            await save_child_info(child, child_info)
            await cache_set_child_info(child, child_info)
            n_new = 1

        # Step 4: Commit link if not already committed
        if not await is_link_committed(parent, child):
            await save_link(parent, child)
            await commit_link(parent, child)

        return n_new

    results = await asyncio.gather(*[process_child(child) for child in children])
    n_new_children = sum([r for r in results if r is not None])
    n_failed = sum([1 for r in results if r is None])

    return n_new_children, n_failed
