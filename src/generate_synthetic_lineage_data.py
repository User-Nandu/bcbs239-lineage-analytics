import pandas as pd
import random
from datetime import datetime, timedelta

# -------------------------
# Configuration
# -------------------------

NUM_NODES = 100
NUM_EDGES = 150

systems = ["Murex", "SAP", "Snowflake", "Oracle", "Axiom"]
jurisdictions = ["US", "EU", "UK", "APAC"]

node_types = [
    "SourceTable",
    "TransformationJob",
    "TargetTable",
    "RegulatoryReport"
]

criticality_levels = ["High", "Medium", "Low"]

# -------------------------
# Generate Nodes
# -------------------------

nodes = []

for i in range(NUM_NODES):

    node_id = f"N{i:03}"

    node_type = random.choice(node_types)

    node = {
        "Node_ID": node_id,
        "Node_Name": f"{node_type}_{i}",
        "Type": node_type,
        "System_Source": random.choice(systems),
        "Criticality": random.choice(criticality_levels),
        "Jurisdiction": random.choice(jurisdictions),
        "CDE_Flag": random.choice(["Yes", "No"])
    }

    nodes.append(node)

nodes_df = pd.DataFrame(nodes)

# -------------------------
# Generate Edges
# -------------------------

edges = []

base_time = datetime.now()

for i in range(NUM_EDGES):

    source = random.choice(nodes_df.Node_ID.tolist())
    target = random.choice(nodes_df.Node_ID.tolist())

    sql_templates = [
        "INSERT INTO Risk_Cube SELECT SUM(exposure) FROM Murex_Trades GROUP BY counterparty",
        "SELECT counterparty, SUM(var) FROM market_positions GROUP BY counterparty",
        "INSERT INTO credit_exposure SELECT customer_id, SUM(balance) FROM loan_book GROUP BY customer_id",
        "SELECT portfolio, SUM(liquidity) FROM treasury_positions GROUP BY portfolio"
    ]

    edge = {
        "ETLJobID": f"JOB_{i:03}",
        "Source_ID": source,
        "Target_ID": target,
        "TransformationSQL": random.choice(sql_templates),
        "ExecutionTimestamp": base_time - timedelta(minutes=random.randint(0, 10000)),
        "ExecutionStatus": random.choice(["Success", "Failure", "Timeout"]),
        "Metadata_Status": random.choice(["Complete", "Incomplete"]),
        "RowsProcessed": random.randint(1000, 1000000),
        "Latency": random.uniform(0.1, 20.0)
    }

    edges.append(edge)

edges_df = pd.DataFrame(edges)

# -------------------------
# Save Files
# -------------------------

nodes_df.to_csv("data/raw/Nodes.csv", index=False)
edges_df.to_csv("data/raw/Edges.csv", index=False)

print("Synthetic dataset generated successfully.")