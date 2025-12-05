"""
Common utilities for Neo4j data operations.

Provides functions for merging nodes based on identifiers and
updating node properties with info dicts.

Identifier Storage:
- Each identifier is stored as a separate node (e.g., PaperIdentifier, AuthorIdentifier, VenueIdentifier)
- Main nodes (Paper, Author, Venue) connect to identifier nodes via HAS_ID relationship
- This allows indexing on identifier values (Neo4j cannot index list properties)
"""

from typing import Any
from neo4j import AsyncSession


def _get_identifier_label(label: str) -> str:
    """Get the identifier node label for a given main node label."""
    return f"{label}Identifier"


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

    id_label = _get_identifier_label(label)

    # Find main nodes through identifier nodes
    query = f"""
        MATCH (n:{label})-[:HAS_ID]->(id:{id_label})
        WHERE id.value IN $identifiers
        WITH DISTINCT n
        OPTIONAL MATCH (n)-[:HAS_ID]->(all_ids:{id_label})
        WITH n, collect(all_ids.value) as id_list
        RETURN n, elementId(n) as element_id, id_list
    """
    result = await tx.run(query, identifiers=list(identifiers))
    nodes = []
    async for record in result:
        node = record["n"]
        nodes.append({
            "element_id": record["element_id"],
            "identifiers": set(record["id_list"]),
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

    id_label = _get_identifier_label(label)

    # Collect all identifiers from all nodes
    all_identifiers = set(new_identifiers)
    for node in nodes:
        all_identifiers.update(node["identifiers"])

    # Merge properties: existing properties, then override with new info
    merged_props = {}
    for node in nodes:
        for key, value in node["properties"].items():
            merged_props[key] = value

    # New info overrides existing
    merged_props.update(new_info)

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

    # Delete other nodes and transfer their relationships (including identifier nodes)
    if len(nodes) > 1:
        other_ids = [n["element_id"] for n in nodes[1:]]
        await _transfer_relationships_and_delete(tx, label, primary_id, other_ids)

    # Ensure all identifiers are linked to the primary node
    # First, get existing identifier values for primary node
    existing_ids_query = f"""
        MATCH (n:{label})-[:HAS_ID]->(id:{id_label})
        WHERE elementId(n) = $primary_id
        RETURN collect(id.value) as existing_ids
    """
    result = await tx.run(existing_ids_query, primary_id=primary_id)
    record = await result.single()
    existing_ids = set(record["existing_ids"]) if record else set()

    # Create new identifier nodes for identifiers not yet linked
    new_ids_to_create = all_identifiers - existing_ids
    if new_ids_to_create:
        for id_value in new_ids_to_create:
            create_id_query = f"""
                MATCH (n:{label})
                WHERE elementId(n) = $primary_id
                CREATE (id:{id_label} {{value: $id_value}})
                CREATE (n)-[:HAS_ID]->(id)
            """
            await tx.run(create_id_query, primary_id=primary_id, id_value=id_value)

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
    id_label = _get_identifier_label(label)

    # Transfer identifier nodes from other nodes to primary node
    # First, collect identifier values from other nodes
    collect_ids_query = f"""
        MATCH (other:{label})-[:HAS_ID]->(id:{id_label})
        WHERE elementId(other) IN $other_ids
        RETURN collect(DISTINCT id.value) as id_values
    """
    result = await tx.run(collect_ids_query, other_ids=other_ids)
    record = await result.single()
    other_id_values = set(record["id_values"]) if record else set()

    # Get existing identifier values for primary node
    existing_ids_query = f"""
        MATCH (n:{label})-[:HAS_ID]->(id:{id_label})
        WHERE elementId(n) = $primary_id
        RETURN collect(id.value) as existing_ids
    """
    result = await tx.run(existing_ids_query, primary_id=primary_id)
    record = await result.single()
    existing_ids = set(record["existing_ids"]) if record else set()

    # Create new identifier nodes for values not yet linked to primary
    new_ids_to_create = other_id_values - existing_ids
    if new_ids_to_create:
        for id_value in new_ids_to_create:
            create_id_query = f"""
                MATCH (n:{label})
                WHERE elementId(n) = $primary_id
                CREATE (id:{id_label} {{value: $id_value}})
                CREATE (n)-[:HAS_ID]->(id)
            """
            await tx.run(create_id_query, primary_id=primary_id, id_value=id_value)

    # Delete identifier nodes connected to other nodes
    delete_ids_query = f"""
        MATCH (other:{label})-[:HAS_ID]->(id:{id_label})
        WHERE elementId(other) IN $other_ids
        DETACH DELETE id
    """
    await tx.run(delete_ids_query, other_ids=other_ids)

    # Transfer incoming relationships (except HAS_ID)
    transfer_in_query = f"""
        MATCH (n:{label}) WHERE elementId(n) = $primary_id
        MATCH (other:{label}) WHERE elementId(other) IN $other_ids
        MATCH (source)-[r]->(other)
        WHERE source <> n AND type(r) <> 'HAS_ID'
        CALL {{
            WITH source, r, n
            WITH source, type(r) as rel_type, properties(r) as rel_props, n
            CALL apoc.create.relationship(source, rel_type, rel_props, n) YIELD rel
            RETURN rel
        }}
        RETURN count(*) as transferred
    """

    # Transfer outgoing relationships (except HAS_ID)
    transfer_out_query = f"""
        MATCH (n:{label}) WHERE elementId(n) = $primary_id
        MATCH (other:{label}) WHERE elementId(other) IN $other_ids
        MATCH (other)-[r]->(target)
        WHERE target <> n AND type(r) <> 'HAS_ID'
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
    id_label = _get_identifier_label(label)
    props = dict(info)

    # Build property string for main node (without identifiers)
    prop_items = []
    params = {}
    for key, value in props.items():
        param_name = f"prop_{key.replace(':', '_').replace('-', '_')}"
        prop_items.append(f"`{key}`: ${param_name}")
        params[param_name] = value

    # Create main node
    if prop_items:
        query = f"""
            CREATE (n:{label} {{{', '.join(prop_items)}}})
            RETURN elementId(n) as element_id
        """
    else:
        query = f"""
            CREATE (n:{label})
            RETURN elementId(n) as element_id
        """
    result = await tx.run(query, **params)
    record = await result.single()
    element_id = record["element_id"]

    # Create identifier nodes and connect them
    for id_value in identifiers:
        id_query = f"""
            MATCH (n:{label})
            WHERE elementId(n) = $element_id
            CREATE (id:{id_label} {{value: $id_value}})
            CREATE (n)-[:HAS_ID]->(id)
        """
        await tx.run(id_query, element_id=element_id, id_value=id_value)

    return element_id


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
