"""
Common utilities for Neo4j data operations.

Provides functions for merging nodes based on identifiers and
updating node properties with info dicts.
"""

from typing import Any
from neo4j import AsyncSession


async def find_nodes_by_identifiers(
    tx,
    label: str,
    identifiers: set[str]
) -> list[dict]:
    """
    Find all nodes with the given label that have any matching identifier.

    Args:
        tx: Neo4j transaction
        label: Node label (Paper, Author, Venue)
        identifiers: Set of identifiers to match

    Returns:
        List of node dicts with element_id, identifiers, and properties
    """
    if not identifiers:
        return []

    query = f"""
        MATCH (n:{label})
        WHERE ANY(id IN n.identifiers WHERE id IN $identifiers)
        RETURN n, elementId(n) as element_id
    """
    result = await tx.run(query, identifiers=list(identifiers))
    nodes = []
    async for record in result:
        node = record["n"]
        nodes.append({
            "element_id": record["element_id"],
            "identifiers": set(node.get("identifiers", [])),
            "properties": dict(node)
        })
    return nodes


async def merge_nodes_into_one(
    tx,
    label: str,
    nodes: list[dict],
    new_identifiers: set[str],
    new_info: dict
) -> str:
    """
    Merge multiple nodes into one, combining identifiers and properties.

    The first node is kept, others are deleted after merging their properties.
    New info values override existing values for the same keys.

    Args:
        tx: Neo4j transaction
        label: Node label
        nodes: List of node dicts from find_nodes_by_identifiers
        new_identifiers: New identifiers to add
        new_info: New info dict (keys override existing)

    Returns:
        element_id of the merged node
    """
    if not nodes:
        raise ValueError("No nodes to merge")

    # Collect all identifiers from all nodes
    all_identifiers = set(new_identifiers)
    for node in nodes:
        all_identifiers.update(node["identifiers"])

    # Merge properties: existing properties, then override with new info
    merged_props = {}
    for node in nodes:
        for key, value in node["properties"].items():
            if key != "identifiers":  # Skip identifiers, handled separately
                merged_props[key] = value

    # New info overrides existing
    merged_props.update(new_info)
    merged_props["identifiers"] = list(all_identifiers)

    # Keep the first node, update it with merged properties
    primary_node = nodes[0]
    primary_id = primary_node["element_id"]

    # Build SET clause for all properties
    set_clauses = []
    params = {"primary_id": primary_id}
    for key, value in merged_props.items():
        param_name = f"prop_{key.replace(':', '_').replace('-', '_')}"
        set_clauses.append(f"n.`{key}` = ${param_name}")
        params[param_name] = value

    if set_clauses:
        update_query = f"""
            MATCH (n:{label})
            WHERE elementId(n) = $primary_id
            SET {', '.join(set_clauses)}
        """
        await tx.run(update_query, **params)

    # Delete other nodes and transfer their relationships
    if len(nodes) > 1:
        other_ids = [n["element_id"] for n in nodes[1:]]
        await _transfer_relationships_and_delete(tx, label, primary_id, other_ids)

    return primary_id


async def _transfer_relationships_and_delete(
    tx,
    label: str,
    primary_id: str,
    other_ids: list[str]
) -> None:
    """
    Transfer all relationships from other nodes to primary node, then delete others.

    Args:
        tx: Neo4j transaction
        label: Node label
        primary_id: element_id of node to keep
        other_ids: element_ids of nodes to delete
    """
    # Transfer incoming relationships
    transfer_in_query = f"""
        MATCH (n:{label}) WHERE elementId(n) = $primary_id
        MATCH (other:{label}) WHERE elementId(other) IN $other_ids
        MATCH (source)-[r]->(other)
        WHERE source <> n
        CALL {{
            WITH source, r, n
            WITH source, type(r) as rel_type, properties(r) as rel_props, n
            CALL apoc.create.relationship(source, rel_type, rel_props, n) YIELD rel
            RETURN rel
        }}
        RETURN count(*) as transferred
    """

    # Transfer outgoing relationships
    transfer_out_query = f"""
        MATCH (n:{label}) WHERE elementId(n) = $primary_id
        MATCH (other:{label}) WHERE elementId(other) IN $other_ids
        MATCH (other)-[r]->(target)
        WHERE target <> n
        CALL {{
            WITH n, r, target
            WITH n, type(r) as rel_type, properties(r) as rel_props, target
            CALL apoc.create.relationship(n, rel_type, rel_props, target) YIELD rel
            RETURN rel
        }}
        RETURN count(*) as transferred
    """

    # Try with APOC, fall back to simpler approach without relationship transfer
    try:
        await tx.run(transfer_in_query, primary_id=primary_id, other_ids=other_ids)
        await tx.run(transfer_out_query, primary_id=primary_id, other_ids=other_ids)
    except Exception:
        # APOC not available, just delete without transferring
        # Relationships will be recreated by subsequent operations
        pass

    # Delete other nodes
    delete_query = f"""
        MATCH (n:{label})
        WHERE elementId(n) IN $other_ids
        DETACH DELETE n
    """
    await tx.run(delete_query, other_ids=other_ids)


async def create_node(
    tx,
    label: str,
    identifiers: set[str],
    info: dict
) -> str:
    """
    Create a new node with the given label, identifiers, and info.

    Args:
        tx: Neo4j transaction
        label: Node label
        identifiers: Set of identifiers
        info: Info dict to store as properties

    Returns:
        element_id of the created node
    """
    props = dict(info)
    props["identifiers"] = list(identifiers)

    # Build property string
    prop_items = []
    params = {}
    for key, value in props.items():
        param_name = f"prop_{key.replace(':', '_').replace('-', '_')}"
        prop_items.append(f"`{key}`: ${param_name}")
        params[param_name] = value

    query = f"""
        CREATE (n:{label} {{{', '.join(prop_items)}}})
        RETURN elementId(n) as element_id
    """
    result = await tx.run(query, **params)
    record = await result.single()
    return record["element_id"]


async def save_node(
    tx,
    label: str,
    identifiers: set[str],
    info: dict
) -> str:
    """
    Save a node: find existing nodes by identifiers, merge if found, create if not.

    This is the main entry point for saving Paper/Author/Venue nodes.

    Args:
        tx: Neo4j transaction
        label: Node label (Paper, Author, Venue)
        identifiers: Set of identifiers
        info: Info dict to store as properties

    Returns:
        element_id of the saved/merged node
    """
    # Find all nodes with matching identifiers
    existing_nodes = await find_nodes_by_identifiers(tx, label, identifiers)

    if existing_nodes:
        # Merge all matching nodes into one
        return await merge_nodes_into_one(tx, label, existing_nodes, identifiers, info)
    else:
        # Create new node
        return await create_node(tx, label, identifiers, info)


async def create_relationship(
    tx,
    from_label: str,
    from_identifiers: set[str],
    to_label: str,
    to_identifiers: set[str],
    rel_type: str
) -> None:
    """
    Create a relationship between two nodes identified by their identifiers.

    If either node doesn't exist, creates a placeholder node with just identifiers.

    Args:
        tx: Neo4j transaction
        from_label: Label of source node
        from_identifiers: Identifiers of source node
        to_label: Label of target node
        to_identifiers: Identifiers of target node
        rel_type: Relationship type (e.g., "AUTHORED", "PUBLISHED_IN", "CITES")
    """
    # Ensure both nodes exist (create placeholders if needed)
    from_nodes = await find_nodes_by_identifiers(tx, from_label, from_identifiers)
    if not from_nodes:
        await create_node(tx, from_label, from_identifiers, {})
        from_nodes = await find_nodes_by_identifiers(tx, from_label, from_identifiers)

    to_nodes = await find_nodes_by_identifiers(tx, to_label, to_identifiers)
    if not to_nodes:
        await create_node(tx, to_label, to_identifiers, {})
        to_nodes = await find_nodes_by_identifiers(tx, to_label, to_identifiers)

    from_id = from_nodes[0]["element_id"]
    to_id = to_nodes[0]["element_id"]

    # Create relationship if not exists
    query = f"""
        MATCH (a:{from_label}) WHERE elementId(a) = $from_id
        MATCH (b:{to_label}) WHERE elementId(b) = $to_id
        MERGE (a)-[r:{rel_type}]->(b)
    """
    await tx.run(query, from_id=from_id, to_id=to_id)
