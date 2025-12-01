"""
Identifier Registry - Core component for managing object identity.

Two objects with any common identifier are considered the same object.
When objects are merged, their identifier sets are combined.
"""

import asyncio

from ..identifier import IdentifierRegistryIface


class MemoryIdentifierRegistry(IdentifierRegistryIface):
    """In-memory implementation of identifier registry using Union-Find."""

    def __init__(self):
        self._lock = asyncio.Lock()
        # Maps identifier -> canonical_id
        self._identifier_to_canonical: dict[str, str] = {}
        # Maps canonical_id -> set of all identifiers
        self._canonical_to_identifiers: dict[str, set[str]] = {}
        # Counter for generating new canonical IDs
        self._counter = 0

    async def get_canonical_id(self, identifiers: set[str]) -> str | None:
        async with self._lock:
            for ident in identifiers:
                if ident in self._identifier_to_canonical:
                    return self._identifier_to_canonical[ident]
            return None

    async def register(self, identifiers: set[str]) -> str:
        async with self._lock:
            # Find all existing canonical IDs that match any identifier
            existing_canonical_ids = set()
            for ident in identifiers:
                if ident in self._identifier_to_canonical:
                    existing_canonical_ids.add(self._identifier_to_canonical[ident])

            if not existing_canonical_ids:
                # No existing match, create new canonical ID
                canonical_id = f"id_{self._counter}"
                self._counter += 1
                self._canonical_to_identifiers[canonical_id] = set(identifiers)
                for ident in identifiers:
                    self._identifier_to_canonical[ident] = canonical_id
                return canonical_id

            # Merge all matching canonical IDs into one
            canonical_ids_list = list(existing_canonical_ids)
            primary_canonical = canonical_ids_list[0]

            # Collect all identifiers from all matching canonical IDs
            all_identifiers = set(identifiers)
            for cid in canonical_ids_list:
                all_identifiers.update(self._canonical_to_identifiers[cid])

            # Update mappings
            self._canonical_to_identifiers[primary_canonical] = all_identifiers
            for ident in all_identifiers:
                self._identifier_to_canonical[ident] = primary_canonical

            # Remove merged canonical IDs
            for cid in canonical_ids_list[1:]:
                del self._canonical_to_identifiers[cid]

            return primary_canonical

    async def get_all_identifiers(self, canonical_id: str) -> set[str]:
        async with self._lock:
            return set(self._canonical_to_identifiers.get(canonical_id, set()))

    async def iterate_canonical_ids(self):
        async with self._lock:
            canonical_ids = list(self._canonical_to_identifiers.keys())
        for cid in canonical_ids:
            yield cid
