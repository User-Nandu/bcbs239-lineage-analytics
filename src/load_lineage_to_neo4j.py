from neo4j import GraphDatabase
import pandas as pd

# -----------------------------
# Neo4j Connection
# -----------------------------

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "cn4125555"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

# -----------------------------
# Load CSV files
# -----------------------------

nodes_df = pd.read_csv("data/raw/Nodes.csv")
edges_df = pd.read_csv("data/raw/Edges.csv")

# -----------------------------
# Create Nodes
# -----------------------------

def create_node(tx, row):
    query = """
    CREATE (n:Node {
        Node_ID: $Node_ID,
        Node_Name: $Node_Name,
        Type: $Type,
        System_Source: $System_Source,
        Criticality: $Criticality,
        Jurisdiction: $Jurisdiction,
        CDE_Flag: $CDE_Flag
    })
    """
    tx.run(query, **row)


# -----------------------------
# Create Relationships
# -----------------------------

def create_edge(tx, row):
    query = """
    MATCH (a:Node {Node_ID: $Source_ID})
    MATCH (b:Node {Node_ID: $Target_ID})
    CREATE (a)-[:DATA_FLOW {
        ETLJobID: $ETLJobID,
        ExecutionStatus: $ExecutionStatus,
        Metadata_Status: $Metadata_Status,
        RowsProcessed: $RowsProcessed,
        Latency: $Latency
    }]->(b)
    """
    tx.run(query, **row)


# -----------------------------
# Insert Nodes
# -----------------------------

with driver.session() as session:
    for _, row in nodes_df.iterrows():
        session.execute_write(create_node, row.to_dict())

print("Nodes inserted.")

# -----------------------------
# Insert Edges
# -----------------------------

with driver.session() as session:
    for _, row in edges_df.iterrows():
        session.execute_write(create_edge, row.to_dict())

print("Edges inserted.")

driver.close()

print("Dataset successfully loaded into Neo4j.")