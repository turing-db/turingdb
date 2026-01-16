
<p align="center">
  <img src="https://github.com/turing-db/turingdb/blob/finalreadmefixes/img/logo_spiral.png" width="350" alt="TuringDB logo">
</p>

## A Blazingly Fast Column-Oriented Graph Database Engine

[Website](https://turingdb.ai/) • [Documentation](https://docs.turingdb.ai/) • [Discord](https://discord.gg/dMM48ns5VY) • [Examples](https://github.com/turing-db/turingdb-examples)

[![CI Ubuntu](https://github.com/turing-db/turingdb/actions/workflows/ci_ubuntu.yml/badge.svg)](https://github.com/turing-db/turingdb/actions/workflows/ci_ubuntu.yml)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white)](https://discord.com/invite/dMM48ns5VY)
[![Docs](https://img.shields.io/badge/Docs-docs.turingdb.ai-brightgreen?style=for-the-badge)](https://docs.turingdb.ai)

<p align="center">
  <img src="https://github.com/turing-db/turingdb/blob/finalreadmefixes/img/newbackgroundgif.gif"/>
</p>

## What is TuringDB?

TuringDB is a high-performance in-memory column-oriented graph database engine designed for analytical and read-intensive workloads. 
Built from scratch in C++.

TuringDB delivers **millisecond query latency** on graphs with millions of nodes and edges. TuringDB is commonly 200x faster than Neo4j on deep multihop queries.


<p align="center">
  <a href="https://www.youtube.com/watch?v=Cpz-I6aR_cw&list=RDCpz-I6aR_cw&start_radio=1">
    <img src="https://github.com/turing-db/turingdb/blob/finalreadmefixes/img/newyoutubeimage.png" width="650" />
  </a>
</p>

#### Visualizing a massive mesh graph with OpenGL accelerated graph visualization

## Why TuringDB?

TuringDB was created to solve real-world performance challenges in critical industries where every millisecond matters:

- **Bioinformatics & Life Sciences** - Analyse complex deep multi-scale biological networks
- **AI & Machine Learning** - Power GraphRAG, knowledge graphs, and agentic AI systems
- **Real-time Analytics** - Build dashboards and simulations with instant query responses
- **Healthcare & Pharma** - Handle sensitive data with GDPR compliance and auditability
- **Financial Services** - Audit trails with git-like versioning for regulatory compliance

## Key Features

### [**Performance-first Architecture**](https://docs.turingdb.ai/query/benchmarks)

- **0.1-50ms query latency** for analytical queries on 10M+ node graphs
- Column-oriented architecture with streaming query processing: nodes and edges are processed in chunks for efficiency
- In-memory graph storage with efficient memory representations

### [**Zero-Lock Concurrency**](https://docs.turingdb.ai/concepts/zero_locking)

- Reads and writes never compete - analytical queries run without locking from writes
- Massive parallelism for dashboards, AI pipelines, and batch processing
- Each transaction executes on its own immutable snapshot. Guarantees Snapshot Isolation.

### [**The First Git-like Versioning System for Graphs**](https://docs.turingdb.ai/concepts/commits)

- Create and commit graph versions, maintain branches, merge changes, and time travel through history
- Perfect for reproducibility, auditability, and regulatory compliance
- Immutable snapshots ensure data integrity

### [**Developer Friendly**](https://docs.turingdb.ai/query/cypher_subset)

- OpenCypher query language
- Python SDK with comprehensive API

## Benchmarks

TuringDB delivers exceptional performance on standard graph workloads 200X faster than Neo4j:

###  Multi-hop Queries From a Set of Seed Nodes
Specs: Machine with AMD EPYC 7232P 8-Core CPU and RAM 64G

```
MATCH (n{displayName: ‘APOE-4’}) RETURN count(n)
```

**Query for 1-hop**: ``` MATCH (n{displayName: ‘APOE-4’})-->(m) RETURN count(n) ```

| Query Depth | Neo4j Mean | TuringDB Mean | Speedup |
|------------|-----------|---------------|---------|
| 1-hop      | 1390 ms   | 12 ms         | 115×    |
| 2-hop      | 1420 ms   | 11 ms         | 129×    |
| 4-hop      | 1568 ms   | 14 ms         | 112×    |
| 7-hop      | 51,264 ms | 172 ms        | 298×    |
| 8-hop      | 98,183 ms | 476 ms        | 206×    |

**Details:** `MATCH` query returning node IDs from a set of 15 seed nodes with an increasing number of hops through outgoing edges.



See our [detailed benchmarks](https://docs.turingdb.ai/query/benchmarks) for more performance data.


## Quickstart

### Requirements

**Install via pip**

* Linux: tested primarily on Ubuntu Jammy 22.04 LTS
* MacOS

**For Build**

TuringDB requires:

For Linux:
- **Linux Ubuntu Jammy** 22.04 LTS or later
- **GCC** version >=11
- **CMake** 3.10 or higher

For MacOS:
- **LLVM Clang** version >=15
- **CMake** 3.10 or higher

### Install TuringDB

Using `pip`:    
```bash
pip install turingdb
```

or using the `uv`  package manager (you will need to create a project first):

```bash
uv add turingdb
```
And then it should be in your $PATH

or using the docker image:
If you can it’s better to use TuringDB within a `uv` project or with `pip install` to avoid loss in latency performance related to the docker deployment.

``` bash
docker run -it turingdbai/turingdb:nightly turingdb
```

### How to run

Run the turingdb binary to use turingdb in interactive mode in the current terminal:
```bash
turingdb
```

To run turingdb in the background as a daemon:
```bash
turingdb -demon
```

The turingdb REST API is running in both interactive and demon mode on http://localhost:6666 by default.

### Visualise the graph you have created in TuringDB
TuringDB WebGL accelerated graph visualizer allows you to visualise large graphs in the browser: [**TuringDB Visualizer**](https://github.com/turing-db/turingdb-visualizer)

#### Example of a biological interaction graph built in TuringDB:
1. Choose on the top right the graph
2. Search & select a node(s) of interest (magnifying glass button on the left)
3. Then go to visualiser (network logo) and you can start exploring paths, expanding neighbours, inspect nodes (parameters, hyperlinks, and texts stored on the nodes)

<p align="center">
  <img src="https://github.com/turing-db/turingdb/blob/finalreadmefixes/img/biologicalgraph.png"/>
</p>

### How to build

1. Build on Ubuntu Jammy or later

2. Clone with submodules
```bash
git clone --recursive https://github.com/turing-db/turingdb.git
./pull.sh
```

3. Install dependencies (run only once)
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

4. Build TuringDB:
```bash
mkdir -p build && cd build

cmake ..
make -j8
make install
```

5. Setup environment:
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

### Explore the full set of notebooks:

[**github.com/turing-db/turingdb-examples**](https://github.com/turing-db/turingdb-examples/tree/main/examples/notebooks/public_version)

#### **Finance & Fraud Detection**

[**Paysim Financial Fraud Detection**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/paysim_financial_fraud_detection.ipynb)

[**Crypto Orbitaal Fraud Detection**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/crypto_orbitaal_fraud_detection.ipynb)

#### **Transport & Supply Chain**

[**London Transport (TfL)**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/london_transport_TfL.ipynb)

[**Supply Chain - ETO Chip Explorer**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/supply_chain_eto-chip-explorer.ipynb)

#### **Healthcare & Life Sciences**

[**Reactome Biological Pathways**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/reactome.ipynb)

[**Healthcare Knowledge Graph**](https://github.com/turing-db/turingdb-examples/blob/main/examples/notebooks/public_version/healthcare_dataset.ipynb)



## Core Concepts

- [**Columnar Storage**](https://docs.turingdb.ai/concepts/columnar_storage): Nodes and edges stored in columns for efficiency and streaming query processing. Brings modern database research ideas to graph databases.
- [**DataParts**](https://docs.turingdb.ai/concepts/dataparts): Immutable data partitions that enable git-like versioning and zero locking between reads and writes
- [**Snapshot Isolation**](https://docs.turingdb.ai/concepts/snapshots): Each query sees a consistent snapshot view of the graph
- [**Zero-locking**](https://docs.turingdb.ai/concepts/zero_locking): Read optimised architecture for high analytic workloads performance

Learn more in our [documentation](https://docs.turingdb.ai/concepts/overview).



## Documentation

- [**Getting Started Guide**](https://docs.turingdb.ai/quickstart) - Your first steps with TuringDB
- [**Query Language Reference**](https://docs.turingdb.ai/query/cypher_subset) - Complete query syntax guide
- [**Python SDK Documentation**](https://docs.turingdb.ai/pythonsdk/get_started) - Integrate TuringDB with Python
- [**Concepts & Architecture**](https://docs.turingdb.ai/concepts/overview) - Deep dive into TuringDB internals
- [**Tutorials & Examples**](https://docs.turingdb.ai/tutorials/create_graph) - Learn by doing



## Community & Support

- [**Discord Community**](https://discord.gg/dMM48ns5VY) - Chat with users and developers
- [**Issue Tracker**](https://github.com/turing-db/turingdb/issues) - Report bugs or request features
- [**Email Support**](mailto:support@turingdb.ai) - Contact the TuringDB team
- [**LinkedIn**](https://www.linkedin.com/company/turingdb-ai) - Follow us for updates

### Contributing

We welcome contributions! Contact our team to contribute to TuringDB: team@turingdb.ai






## License

TuringDB Community Edition is licensed under the BSL **License**

See [LICENSE](https://www.notion.so/turingbio/LICENSE) for more details.



## Contact **TuringDB Team**

- 🌐 Website: [turingdb.ai](https://turingdb.ai/)
- 📧 Email: team@turingdb.ai
- 💼 LinkedIn: [TuringDB](https://www.linkedin.com/company/turingdb-ai)

---

<div align="center">

### **Built with ❤️ for the graph database community**

**Star us on GitHub** - it helps the project grow!

[⬆ Back to Top](https://github.com/turing-db/turingdb)

</div>
