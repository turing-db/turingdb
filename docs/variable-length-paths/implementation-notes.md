# Implementation Notes: Execution Algorithms

This document discusses execution strategies for quantified path patterns in a columnar query engine.

---

## Columnar Execution Model

### Batch Processing

Our query engine processes data in **chunks** (batches of rows). For quantified path patterns:

**Input:** A batch of seed nodes `[n₁, n₂, ..., nₖ]`  
**Output:** A batch of (start, end) pairs `[(n₁, m₁), (n₂, m₂), ...]`

**Key insight:** When processing multiple seeds simultaneously, we can share traversal work:
```
Seeds: [A, B]
Graph: A -> C -> D
       B -> C -> D

Without optimization:
  - Traverse from A: A->C, A->C->D
  - Traverse from B: B->C, B->C->D  (redundant C->D exploration)

With optimization:
  - Track already-explored paths from C
  - Reuse C->D traversal for both A and B
```

---

## Core Algorithms

### 1. Breadth-First Search (BFS)

**Best for:** Shortest paths, bounded quantifiers

**Algorithm:**
```
Input: seeds = [n₁, n₂, ..., nₖ], quantifier {min, max}
Output: (seed, destination) pairs

1. Initialize frontier[0] = seeds (hop 0)
2. Initialize visited = empty set
3. For hop = 1 to max:
     For each node in frontier[hop-1]:
       For each neighbor via matching relationship:
         If neighbor not in visited (if no-cycles):
           Add neighbor to frontier[hop]
           If hop >= min:
             Emit (seed, neighbor)
         Mark neighbor as visited
```

**Advantages:**
- Finds shortest paths first
- Natural depth tracking
- Memory-efficient for bounded max

**Disadvantages:**
- Poor for deep, sparse graphs
- High memory for high-branching graphs

**Batch optimization:**
```
When processing multiple seeds:
1. Group seeds by their frontier nodes at each level
2. For shared frontier nodes, expand only once
3. Propagate results back to all relevant seeds

Example:
  Seeds: [A, B]
  Hop 1: A->C, B->C (C is shared)
  Hop 2: Expand C once, attribute results to both A and B
```

---

### 2. Depth-First Search (DFS)

**Best for:** Deep paths, unbounded quantifiers, low branching

**Algorithm:**
```
Input: seed, quantifier {min, max}, path_so_far
Output: (seed, destination) pairs

1. For each neighbor via matching relationship:
     If max not reached AND neighbor not in path_so_far (if no-cycles):
       new_path = path_so_far + [neighbor]
       If length(new_path) >= min:
         Emit (seed, neighbor)
       If length(new_path) < max:
         DFS(neighbor, quantifier, new_path)
```

**Advantages:**
- Low memory (stack-based)
- Good for deep, narrow traversals
- Early termination opportunities

**Disadvantages:**
- May find long paths before short ones
- Harder to batch effectively
- Risk of stack overflow for very deep paths

**Batch optimization:**
```
Limited batching opportunities in pure DFS.
Consider hybrid approach:
1. Start with BFS for first k levels
2. Switch to DFS for deeper exploration
3. Threshold based on branching factor
```

---

### 3. Bidirectional Search

**Best for:** Bounded quantifiers with known endpoints, moderate hop counts

**Algorithm:**
```
Input: seeds, quantifier {min, max}
Output: (seed, destination) pairs

1. Forward frontier: BFS from seeds up to max/2 hops
2. Backward frontier: BFS from all candidate destinations up to max/2 hops
3. Find intersections between frontiers
4. Validate total path length is within [min, max]
5. Emit (seed, destination) for valid paths
```

**Advantages:**
- Reduces search space dramatically for mid-range hop counts
- O(B^(k/2)) instead of O(B^k) where B is branching factor

**Disadvantages:**
- Requires knowing potential destinations in advance
- More complex implementation
- Not applicable for `+` or `*` (unbounded endpoints)

**When to use:**
- Pattern: `(n:Label1)-[:REL]->{5,10}(m:Label2)`
- Both start and end have specific labels/constraints
- Can precompute destination candidates

**Batch optimization:**
```
1. Collect all seeds in batch
2. Collect all destination candidates with matching labels
3. Run forward BFS from all seeds
4. Run backward BFS from all destinations
5. Match frontiers efficiently using hash joins
```

---

### 4. Index-Based Pruning

**Best for:** Selective filters, labeled nodes, property constraints

**Strategy 1: Label-based pruning**
```
Query: (n:Person)-[:KNOWS]->+(m:Manager)

1. Use label index to get all Manager nodes
2. Run backward BFS from Manager nodes
3. Check if any seeds are reached within hop bounds
4. Only explore paths that can reach a Manager

Avoids exploring paths that dead-end at non-Manager nodes.
```

**Strategy 2: Property-based pruning**
```
Query: (n)-[:KNOWS {since: 2020}]->+(m)

1. Use relationship property index to filter edges
2. Only traverse edges with since = 2020
3. Build filtered adjacency lists upfront

Avoids evaluating property predicates during traversal.
```

**Strategy 3: Degree-based pruning**
```
For high-degree nodes (hubs):
1. Maintain statistics on node degrees
2. Prioritize low-degree nodes in traversal
3. For hubs, use sampling or limits
4. Prevents explosion from celebrity nodes

Example: Social network with celebrity accounts
```

**Batch optimization:**
```
1. Identify all relevant indexes for the query
2. Precompute filtered edges for the entire batch
3. Build batch-specific adjacency structures
4. Reuse across all seeds in the batch
```

---

### 5. Hybrid Approach

**Recommended strategy for most queries:**

```
Phase 1: Shallow BFS (0 to k hops, typically k=3)
  - Explore all seeds in parallel
  - Track common frontiers
  - Share expansion work for overlapping paths

Phase 2: Adaptive strategy based on frontier size
  - If frontier is small: Continue BFS
  - If frontier is large: Switch to sampling or DFS per seed
  - If approaching max hops: Use bidirectional if possible

Phase 3: Deduplication
  - Collect all (seed, destination) pairs
  - Remove duplicates (if path uniqueness = one per endpoint pair)
  - Apply any additional filters
```

**Decision thresholds:**
```
frontier_size < 1000: BFS
1000 <= frontier_size < 10000: Selective DFS
frontier_size >= 10000: Sample or warn user
```

---

## Cycle Handling Strategies

### No-Cycle Enforcement

**Track visited nodes per path:**
```
Algorithm: DFS with visited set

For each path expansion:
  visited_in_this_path = set of nodes in current path
  For each neighbor:
    If neighbor not in visited_in_this_path:
      Expand to neighbor
```

**Memory:** O(max_hops) per active path  
**Performance:** Minimal overhead

### Cycle Allowing with Limits

**Track visit count per node:**
```
Algorithm: BFS with visit limits

For each node in frontier:
  visit_count[node] += 1
  If visit_count[node] <= max_visits_per_node:
    Expand from node
  Else:
    Prune (prevent infinite cycles)
```

**Prevents infinite loops while allowing limited cycles**

---

## Batch-Specific Optimizations

### 1. Frontier Merging

When multiple seeds reach the same node at the same hop level:
```
Seeds: [A, B, C]
All reach node X at hop 2

Instead of:
  - Expand X for A
  - Expand X for B  (duplicate work)
  - Expand X for C  (duplicate work)

Do:
  - Expand X once
  - Attribute results to {A, B, C}
  - Maintain provenance: (seed_id, node, hop_level)
```

**Implementation:**
```
frontier = {
  hop_level -> {
    node -> set of seeds that reached it
  }
}

For each hop:
  For each node in frontier[hop]:
    seeds = frontier[hop][node]
    neighbors = expand(node)
    For each neighbor in neighbors:
      frontier[hop+1][neighbor].add_all(seeds)
```

**Memory trade-off:** Store seed sets per frontier node vs. duplicate expansions

---

### 2. Adjacency List Caching

For a batch of seeds, precompute relevant adjacency lists:
```
1. Collect all nodes likely to be visited (heuristic or first pass)
2. Bulk-load their adjacency lists into memory
3. Filter edges by relationship type/properties
4. Cache for duration of batch processing

Columnar benefit:
  - Sequential reads instead of random access
  - Cache-friendly access patterns
  - Amortize I/O across all seeds
```

---

### 3. Work Stealing

In parallel execution:
```
1. Partition seeds across worker threads
2. Each worker maintains its own frontier
3. When a worker completes its seeds early:
   - Steal frontier nodes from busy workers
   - Assist in expansion
   - Return results to appropriate seeds

Balances load across threads for varying seed complexity.
```

---

### 4. Result Streaming

For large result sets:
```
Don't accumulate all (seed, destination) pairs in memory.

Instead:
1. Emit results as soon as they satisfy [min, max] constraints
2. Stream to next query operator
3. Apply backpressure if downstream is slow

Columnar advantage:
  - Results already in columnar format
  - No additional transformation needed
  - Minimal memory footprint
```

---

## Memory Management

### Frontier Size Control

**Problem:** BFS frontier can explode exponentially

**Solution 1: Frontier size limits**
```
If frontier[hop] > threshold:
  - Switch to sampling: randomly select subset
  - Warn user: "Result may be incomplete"
  - Or: prune lowest-priority nodes (e.g., by degree)
```

**Solution 2: Spill to disk**
```
When memory pressure detected:
  - Serialize oldest frontier to disk
  - Process newer frontiers in memory
  - Load back when needed
  
Trade CPU (serialization) for memory.
```

### Path Tracking

**Problem:** For cycle prevention, need to track per-path state

**Solution: Compact representation**
```
Instead of storing full path:
  - Store path as bitset (node_id -> bit)
  - Or: Bloom filter (probabilistic, low memory)
  - Or: Hash of path (may have collisions)

Trade accuracy for memory efficiency.
```

---

## Performance Characteristics

### Time Complexity

**BFS:**
- Best case: O(min_hops × branching_factor)
- Average case: O(B^k) where B = branching factor, k = hops
- Worst case: O(V) for unbounded in fully connected graph

**DFS:**
- Best case: O(min_hops)
- Average case: O(B^k)
- Worst case: Exponential with high branching and deep max

**Bidirectional:**
- Best case: O(B^(k/2))
- Average case: O(B^(k/2))
- Worst case: O(V) if no intersection

### Space Complexity

**BFS:** O(B^k) for frontier storage
**DFS:** O(k) for path stack
**Bidirectional:** O(2 × B^(k/2)) for both frontiers

### Real-World Performance

**Small graphs (< 100K nodes):**
- BFS is almost always fastest
- Memory is not a concern
- Full expansion is acceptable

**Medium graphs (100K - 10M nodes):**
- Hybrid approach works best
- Index-based pruning essential
- Monitor frontier sizes

**Large graphs (> 10M nodes):**
- Must use aggressive pruning
- Consider approximate results
- Sampling may be necessary
- Parallel execution critical

---

## Query-Specific Optimizations

### Pattern: `(n)-[:TYPE]->+(m)`

**Unbounded, no constraints**

Strategy:
1. Use BFS up to default max (e.g., 100 hops)
2. Merge frontiers for batched seeds
3. Consider label constraints on m if present

### Pattern: `(n)-[:TYPE]->{5,10}(m:Label)`

**Bounded, destination constrained**

Strategy:
1. Use bidirectional search
2. Start backward from all nodes with :Label
3. Meet in the middle
4. Validate path length

### Pattern: `(n)-[:TYPE]->*(m)`

**Zero-or-more (includes seed)**

Strategy:
1. Immediately emit (seed, seed) for each seed
2. Then run standard BFS for k >= 1

### Pattern: `(n)-[:TYPE {prop: val}]->+(m)`

**Property filter**

Strategy:
1. Pre-filter edges using property index
2. Build filtered adjacency lists
3. Run BFS on filtered graph
4. Much faster than checking properties during traversal

---

## Monitoring and Observability

### Metrics to Track

**Per-query:**
- Frontier sizes at each hop level
- Number of paths explored
- Number of results emitted
- Time spent per hop
- Memory usage peak

**Per-batch:**
- Number of seeds processed
- Shared work percentage (frontier merging effectiveness)
- Cache hit rate (adjacency lists)
- Pruning effectiveness (edges skipped)

### Query Warnings

Emit warnings for:
- Frontier exceeding threshold (potential performance issue)
- Max hop limit reached without exhausting paths
- Sampling activated due to memory pressure
- High-degree nodes encountered (hubs)

---

## Future Optimizations

### 1. Transitive Closure Materialization

For frequently queried relationship types:
```
Precompute and store transitive closure
Query-time: O(1) lookup instead of traversal
Trade-off: Storage space vs. query time
```

### 2. Graph Partitioning

For distributed graphs:
```
Partition by community structure
Minimize cross-partition edges
Process partitions in parallel
Aggregate results
```

### 3. GPU Acceleration

For highly parallel traversals:
```
Represent frontier as bit vectors
Use GPU for parallel neighbor expansion
Works well for BFS on large graphs
```

### 4. Learned Heuristics

Use query history to learn:
- Optimal algorithm choice per query pattern
- Expected frontier sizes
- Effective pruning strategies
- When to switch between algorithms

---

## Recommendations

### For Implementation

**Phase 1: Start simple**
- Implement BFS with no-cycle enforcement
- Default max hop limit: 100
- Batch frontier merging
- Basic index-based pruning

**Phase 2: Add adaptivity**
- Monitor frontier sizes
- Switch to DFS when beneficial
- Implement bidirectional for bounded patterns

**Phase 3: Optimize**
- Advanced index usage
- Parallel execution
- Spill-to-disk for large frontiers

### Configuration Parameters

Expose these as tunable:
```
max_hops_default = 100
frontier_size_threshold = 10000
enable_frontier_merging = true
enable_bidirectional = true (for bounded patterns)
enable_sampling = false (conservative default)
max_memory_per_query = 1GB
```

### Testing Focus

Prioritize testing:
1. Small linear paths (correctness)
2. Cycle graphs (cycle handling)
3. High-degree nodes (performance)
4. Disconnected graphs (correctness)
5. Large batches (batch optimization effectiveness)

---

## See Also

- [Phase 1 Specification](phase-1-spec.md) - Feature requirements
- [Testing Strategy](testing-strategy.md) - Test cases and coverage
- [Overview](overview.md) - General context
