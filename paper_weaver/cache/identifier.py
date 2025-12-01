"""
Identifier Registry - Core component for managing object identity.

Two objects with any common identifier are considered the same object.
When objects are merged, their identifier sets are combined.
"""

from abc import ABCMeta, abstractmethod
from typing import AsyncIterator


class IdentifierRegistryIface(metaclass=ABCMeta):
    """
    Interface for identifier registry that manages object identity.

    Key concepts:
    - Each object has a set of identifiers
    - Two objects with any common identifier are the same object
    - When merging, all identifiers are combined under one canonical ID
    """

    @abstractmethod
    async def get_canonical_id(self, identifiers: set[str]) -> str | None:
        """
        Get the canonical ID for a set of identifiers.
        Returns None if no identifier is registered.
        """
        raise NotImplementedError

    @abstractmethod
    async def register(self, identifiers: set[str]) -> str:
        """
        Register identifiers and return the canonical ID.
        If any identifier is already registered, merge all and return existing canonical ID.
        Otherwise, create a new canonical ID.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_all_identifiers(self, canonical_id: str) -> set[str]:
        """Get all identifiers associated with a canonical ID."""
        raise NotImplementedError

    @abstractmethod
    def iterate_canonical_ids(self) -> AsyncIterator[str]:
        """Async iterator over all canonical IDs."""
        raise NotImplementedError
