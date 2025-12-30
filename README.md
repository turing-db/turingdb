
![TuringDB logo](https://github.com/turing-db/turingdb/blob/main/logo_spiral.png)

# TuringDB

[![CI Ubuntu](https://github.com/turing-db/turingdb/actions/workflows/ci_ubuntu.yml/badge.svg)](https://github.com/turing-db/turingdb/actions/workflows/ci_ubuntu.yml)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white)](https://discord.com/invite/dMM48ns5VY)
[![Docs](https://img.shields.io/badge/Docs-docs.turingdb.ai-brightgreen?style=for-the-badge)](https://docs.turingdb.ai)

[Website](https://turingdb.ai/) • [Documentation](https://docs.turingdb.ai/) • [Discord](https://discord.gg/dMM48ns5VY) • [Examples](https://github.com/turing-db/turingdb-examples)

---

## What is TuringDB?

TuringDB is a high-performance in-memory column-oriented graph database engine designed for analytical and read-intensive workloads. 
Built from scratch in C++.

TuringDB delivers **millisecond query latency** on graphs with millions of nodes and edges. TuringDB is commonly 200x faster than Neo4j on deep multihop queries.

<div align="center">

[https://www.youtube.com/watch?v=Cpz-I6aR_cw&list=RDCpz-I6aR_cw&start_radio=1](https://www.youtube.com/watch?v=Cpz-I6aR_cw&list=RDCpz-I6aR_cw&start_radio=1)

</div>

## Why TuringDB?

TuringDB was created to solve real-world performance challenges in critical industries where every millisecond matters:

- **Bioinformatics & Life Sciences** - Analyse complex deep multi-scale biological networks
- **AI & Machine Learning** - Power GraphRAG, knowledge graphs, and agentic AI systems
- **Real-time Analytics** - Build dashboards and simulations with instant query responses
- **Healthcare & Pharma** - Handle sensitive data with GDPR compliance and auditability
- **Financial Services** - Audit trails with git-like versioning for regulatory compliance

---

## Key Features

### **Extreme Performance**

- **0.1-50ms query latency** for analytical queries on 10M+ node graphs
- Column-oriented architecture with streaming query processing: nodes and edges are processed in chunks for efficiency
- In-memory graph storage with efficient memory representations

### **Zero-Lock Concurrency**

- Reads and writes never compete - analytical queries run without locking from writes
- Massive parallelism for dashboards, AI pipelines, and batch processing
- Each transaction executes on its own immutable snapshot. Guarantees Snapshot Isolation.

### **Git-Like Versioning**

- Create and commit graph versions, maintain branches, merge changes, and time travel through history
- Perfect for reproducibility, auditability, and regulatory compliance
- Immutable snapshots ensure data integrity

### **Rich Graph Semantics & Metadata**

- Unlimited properties per node and edge (up to 1MB per node)
- Store large text chunks and numerical values for comprehensive context
- Ideal for LLM integration, digital twins, and multi-layer graphs

---

## Quickstart

### Requirements

### How to build

1. Clone with submodules
```bash
git clone --recursive https://github.com/turing-db/turingdb.git
./pull.sh
```

2. Install dependencies (run only once)
```bash
./dependencies.sh
```
This script will automatically build or install:
- Curl and OpenSSL
- GNU Bison and Flex
- Boost 
- OpenBLAS
- AWS SDK for C++
- Faiss vector search library

3. Build TuringDB:
```bash
mkdir -p build && cd build

cmake ..
make -j8
make install
```

4. Setup environment:
```bash
source setup.sh
```
This adds turingdb binaries to the $PATH of the current shell.

## Usage Example

### Create and Query a Graph

```
// Create nodes
CREATE (alice:Person {name: "Alice", age: 30})
CREATE (bob:Person {name: "Bob", age: 25})
CREATE (computers:Interest {name: "Computers", hasPhD: true})

// Create relationships
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
CREATE (a)-[:KNOWS {since: 2020}]->(b)

MATCH (a:Person {name: "Alice"}), (i:Interest {name: "Computers"})
CREATE (a)-[:INTERESTED_IN]->(i)

// Query the graph
MATCH (a:Person)-->(b)
WHERE a.age > 25
RETURN a.name, b.name

```

### Python SDK

```python
from turingdb import TuringDB

# Connect to TuringDB
db = TuringDB(host="localhost", port=XXX)

# Execute query
result = db.query("""
    MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest)
    RETURN p.name, i.name
""")

for record in result:
    print(f"{record['p.name']} is interested in {record['i.name']}")
```

## Use cases Notebooks

A collection of notebooks demonstrating how to use TuringDB for real-world analytical examples.

 Each notebook focuses on a different domain and use case, from fraud detection to biological graph exploration, leveraging the performance and versioning capabilities of TuringDB.

Explore the full set of notebooks:

[**github.com/turing-db/turingdb-examples**](https://github.com/turing-db/turingdb-examples/tree/main/examples/notebooks/public_version)

### **Domain: Finance & Fraud Detection**

[**Paysim Financial Fraud Detection**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/paysim_financial_fraud_detection.ipynb)

[**Crypto Orbitaal Fraud Detection**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/crypto_orbitaal_fraud_detection.ipynb)

### **Domain: Transport & Supply Chain**

[**London Transport (TfL)**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/london_transport_TfL.ipynb)

[**Supply Chain - ETO Chip Explorer**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/supply_chain_eto-chip-explorer.ipynb)

### **Domain: Healthcare & Life Sciences**

[**Reactome Biological Pathways**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/reactome.ipynb)

[**Healthcare Knowledge Graph**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/healthcare_dataset.ipynb)

---

## Core Concepts

- **Columnar Storage**: Nodes and edges stored in columns for efficiency and streaming query processing. Brings modern database research ideas to graph databases.
- **DataParts**: Immutable data partitions that enable git-like versioning and zero locking between reads and writes
- **Snapshot Isolation**: Each query sees a consistent snapshot view of the graph
- **Zero-locking: read optimised architecture for high analytic workloads performance**

Learn more in our [documentation](https://docs.turingdb.ai/concepts/overview).

---

## Documentation

- [**Getting Started Guide**](https://docs.turingdb.ai/quickstart) - Your first steps with TuringDB
- [**Query Language Reference**](https://docs.turingdb.ai/query/cypher_subset) - Complete query syntax guide
- [**Python SDK Documentation**](https://docs.turingdb.ai/pythonsdk/get_started) - Integrate TuringDB with Python
- [**Concepts & Architecture**](https://docs.turingdb.ai/concepts/overview) - Deep dive into TuringDB internals
- [**Tutorials & Examples**](https://docs.turingdb.ai/tutorials/create_graph) - Learn by doing

---

## Community & Support

- [**Discord Community**](https://discord.gg/dMM48ns5VY) - Chat with users and developers
- [**Issue Tracker**](https://github.com/turing-db/turingdb/issues) - Report bugs or request features
- [**Email Support**](mailto:support@turingdb.ai) - Contact the TuringDB team
- [**LinkedIn**](https://www.linkedin.com/company/turingdb-ai) - Follow us for updates

### Contributing

We welcome contributions! Contact our team to contribute to TuringDB: team@turingdb.ai

---

## Benchmarks

TuringDB delivers exceptional performance on standard graph workloads 200X faster than Neo4j:

See our [detailed benchmarks](https://docs.turingdb.ai/query/benchmarks) for more performance data.

---

## License

TuringDB Community Edition is licensed under the BSL **License**

See [LICENSE](https://www.notion.so/turingbio/LICENSE) for details.

---

## Contact **TuringDB Team**

- 🌐 Website: [turingdb.ai](https://turingdb.ai/)
- 📧 Email: team@turingdb.ai
- 💼 LinkedIn: [TuringDB](https://www.linkedin.com/company/turingdb-ai)

---

<div align="center">

**Built with ❤️ for the graph database community**

**Star us on GitHub** - it helps the project grow!

[⬆ Back to Top](https://www.notion.so/Github-README-2d33aad664c8801cbb62d27a734ec319?pvs=21)

</div>
