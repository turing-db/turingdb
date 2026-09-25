"""
The graphs the benchmark asks its questions of, and the questions themselves.

Most workloads sweep one pattern over a list of hop bounds, unbounded included, in two
modes: the walk counts every trail the pattern matches, the search counts the distinct
nodes it ends on. `reactome-paths` adds the row-returning and fixed-depth questions of
docs/path_bench.md, and `reactome-hops` times one expansion at each exact hop count. Every
query is plain openCypher - a range literal on the relationship, `all()` for a predicate
on every hop - and is sent to each database verbatim, except ladybug, whose dialect the
client translates into.

A fixture says where each database keeps the same graph: a turing dir and graph name, a
bolt uri, a ladybug database directory, and a falkor graph key. `ladybugNodeTable` names
the one table the fixture packs every label into, empty where a label is its own table.
"""

from dataclasses import dataclass

SIGNAL_TRANSDUCTION = "R-HSA-162582"
HUB_REACTION = "R-HSA-2993780"
DEEPEST_HUMAN_COMPLEX = "R-HSA-6814275"

HOP_COUNTS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 100)

RING_SEED = 796580
RING_DEPTH = 7


@dataclass(frozen=True)
class Fixture:
    turingDir: str
    graph: str
    memgraphURI: str
    ladybugDB: str
    ladybugNodeTable: str
    falkorGraph: str


@dataclass(frozen=True)
class Query:
    group: str
    queryID: str
    question: str
    cypher: str
    sweep: str = ""


@dataclass(frozen=True)
class Workload:
    name: str
    description: str
    fixture: Fixture
    groups: dict
    queries: tuple


def rangeLiteral(bound):
    if bound is None:
        return "*"

    return f"*1..{bound}"


def boundLabel(bound):
    if bound is None:
        return "unbounded"
    elif bound == 1:
        return "up to 1 hop"

    return f"up to {bound} hops"


def hopLabel(hops):
    return "1 hop" if hops == 1 else f"{hops} hops"


# One pattern over every bound, counted as trails and as distinct ends. `pattern` takes
# the range literal to write inside the relationship brackets.
def sweep(group, prefix, question, pattern, end, bounds, searchBounds=None):
    queries = []

    for mode in ("walk", "search"):
        aggregate = f"count(DISTINCT {end})" if mode == "search" else f"count({end})"
        modeBounds = searchBounds if mode == "search" and searchBounds is not None else bounds

        for bound in modeBounds:
            queries.append(Query(group,
                                 f"{prefix}_{mode}_{bound or 'inf'}",
                                 f"{question}, {boundLabel(bound)}, {mode}",
                                 f"MATCH {pattern(rangeLiteral(bound))} RETURN {aggregate}",
                                 f"{prefix}_{mode}"))

    return queries


# One query per exact hop count; `build` takes the count and writes the whole query.
def hopSweep(group, prefix, question, build):
    return [Query(group, f"{prefix}_{hops}", f"{question}, {hopLabel(hops)}", build(hops), prefix) for hops in HOP_COUNTS]


def reactomeQueries():
    queries = [
        Query("S", "st_seed", "Find Signal Transduction by accession",
              f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}}) RETURN count(p)"),

        Query("S", "hub_seed", f"Find reaction {HUB_REACTION} by accession",
              f"MATCH (r:Reaction {{stId:'{HUB_REACTION}'}}) RETURN count(r)"),
    ]

    queries += sweep("A", "st_events", "Sub-events of Signal Transduction",
                     lambda hops: f"(p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[e:hasEvent{hops}]->(a:Event)",
                     "a",
                     (1, 2, 3, 4, 6, None))

    queries += sweep("B", "hub_cascade", f"Reactions upstream of {HUB_REACTION}",
                     lambda hops: f"(r:Reaction {{stId:'{HUB_REACTION}'}})<-[e:precedingEvent{hops}]-(d:Reaction)",
                     "d",
                     (1, 2, 3, 4, 6, None))

    queries += sweep("C", "tlp_events", "Sub-events of every top-level pathway",
                     lambda hops: f"(p:TopLevelPathway)-[e:hasEvent{hops}]->(z:Event)",
                     "z",
                     (1, 2, 3, 4, 6, None))

    queries += sweep("D", "tlp_ball", "Anything reachable from a top-level pathway",
                     lambda hops: f"(p:TopLevelPathway)-[e{hops}]->(m)",
                     "m",
                     (1, 2, 3, None))

    return tuple(queries)


def pathBenchQueries():
    signalTransduction = f"(p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})"
    hub = f"(r:Reaction {{stId:'{HUB_REACTION}'}})"
    deepestComplex = f"(c:Complex {{stId:'{DEEPEST_HUMAN_COMPLEX}'}})"

    queries = [
        Query("A", "seed", "Find Signal Transduction by accession",
              f"MATCH {signalTransduction} RETURN p.displayName"),

        Query("A", "st_d1", "Its direct sub-events (depth 1)",
              f"MATCH {signalTransduction}-[:hasEvent*1..1]->(a:Event) RETURN a.stId"),

        Query("A", "st_d3", "Its sub-events exactly 3 levels down",
              f"MATCH {signalTransduction}-[:hasEvent*3..3]->(c:Event) RETURN c.stId"),

        Query("A", "st_d4", "Its sub-events exactly 4 levels down",
              f"MATCH {signalTransduction}-[:hasEvent*4..4]->(d:Event) RETURN d.stId"),

        Query("A", "cascade_d4", f"Reactions exactly 4 steps downstream of hub {HUB_REACTION}",
              f"MATCH {hub}<-[:precedingEvent*4..4]-(d:Reaction) RETURN d.stId"),

        Query("A", "complex_d2", f"Sub-units of complex {DEEPEST_HUMAN_COMPLEX}, exactly 2 levels in",
              f"MATCH {deepestComplex}-[:hasComponent*2..2]->(b) RETURN b.stId"),

        Query("B", "tlp_typed_d2_all", "Events two hasEvent levels under all 415 top-level pathways",
              "MATCH (p:TopLevelPathway)-[:hasEvent*2..2]->(z:Event) RETURN count(z)"),

        Query("B", "tlp_typed_d3_all", "Events three hasEvent levels under all of them",
              "MATCH (p:TopLevelPathway)-[:hasEvent*3..3]->(z:Event) RETURN count(z)"),

        Query("B", "tlp_typed_d4_all", "Events four hasEvent levels under all of them",
              "MATCH (p:TopLevelPathway)-[:hasEvent*4..4]->(z:Event) RETURN count(z)"),

        Query("B", "tlp_typed_d5_all", "Events five hasEvent levels under all of them",
              "MATCH (p:TopLevelPathway)-[:hasEvent*5..5]->(z:Event) RETURN count(z)"),

        Query("B", "tlp_typed_d6_all", "Events six hasEvent levels under all of them",
              "MATCH (p:TopLevelPathway)-[:hasEvent*6..6]->(z:Event) RETURN count(z)"),

        Query("B", "complex_tree_all", "Every sub-unit two levels inside every complex",
              "MATCH (c:Complex)-[:hasComponent*2..2]->(b) RETURN count(b)"),

        Query("B", "preceding_d3", "Reaction triples three precedingEvent steps apart",
              "MATCH (r:Reaction)<-[:precedingEvent*3..3]-(c:Reaction) RETURN count(c)"),

        Query("B", "tlp_two_hop", "Reactions two hasEvent levels under every top-level pathway",
              "MATCH (tlp:TopLevelPathway)-[:hasEvent]->(p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent) RETURN count(r)"),

        Query("B", "inputs", "Every reaction's input entities",
              "MATCH (r:ReactionLikeEvent)-[:input]->(p:PhysicalEntity) RETURN count(p)"),

        Query("C", "untyped_1_2", "Everything within 2 hops of the 415 top-level pathways",
              "MATCH (p:TopLevelPathway)-[e*1..2]->(m) RETURN count(m)"),

        Query("C", "untyped_1_3", "Everything within 3 hops of them",
              "MATCH (p:TopLevelPathway)-[e*1..3]->(m) RETURN count(m)"),

        Query("C", "untyped_1_3_event", "Events within 3 hops of them",
              "MATCH (p:TopLevelPathway)-[e*1..3]->(m:Event) RETURN count(m)"),

        Query("C", "reaction_2hop", "Everything within 2 hops of all 83,459 reactions",
              "MATCH (r:Reaction)-[e*1..2]->(m) RETURN count(m)"),

        Query("C", "untyped_undirected", "Everything within 2 hops in either direction",
              "MATCH (p:TopLevelPathway)-[e*1..2]-(m) RETURN count(m)"),

        Query("D", "st_all", "Signal Transduction's entire sub-event tree, any depth",
              f"MATCH {signalTransduction}-[:hasEvent*]->(a:Event) RETURN a.stId"),

        Query("D", "st_all_reactions", "Every reaction it eventually decomposes into",
              f"MATCH {signalTransduction}-[:hasEvent*]->(r:Reaction) RETURN r.stId"),

        Query("D", "all_tlp_reactions", "Every reaction under every top-level pathway",
              "MATCH (p:TopLevelPathway)-[:hasEvent*]->(r:Reaction) RETURN p.stId, r.stId"),

        Query("D", "ancestor", f"Which top-level pathway contains reaction {HUB_REACTION}",
              f"MATCH {hub}<-[:hasEvent*]-(p:TopLevelPathway) RETURN p.displayName"),

        Query("D", "cascade_1_4", "Reactions up to 4 steps downstream of that hub",
              f"MATCH {hub}<-[:precedingEvent*1..4]-(d:Reaction) RETURN d.stId"),

        Query("D", "cascade_distinct", "All reactions downstream of that hub, any distance",
              f"MATCH {hub}<-[:precedingEvent*]-(d:Reaction) RETURN DISTINCT d.stId"),

        Query("D", "complex_all", "That complex's whole sub-unit tree",
              f"MATCH {deepestComplex}-[:hasComponent*]->(b) RETURN b.stId"),

        Query("D", "shared_input", "Reaction pairs of one pathway sharing an input entity",
              "MATCH (a:Pathway)-[:hasEvent]->(r1:ReactionLikeEvent), (a)-[:hasEvent]->(r2:ReactionLikeEvent), "
              "(r1)-[:input]->(e:PhysicalEntity), (r2)-[:input]->(e) RETURN count(e)"),
    ]

    queries += sweep("E", "cascade", f"Reactions downstream of hub {HUB_REACTION}",
                     lambda hops: f"{hub}<-[:precedingEvent{hops}]-(z:Reaction)",
                     "z",
                     (4, 8, 16, 24, 32, 40, 44),
                     (4, 8, 16, 24, 32, 40, 44, 100, None))

    queries += sweep("E", "st", "Signal Transduction's sub-events",
                     lambda hops: f"{signalTransduction}-[:hasEvent{hops}]->(z:Event)",
                     "z",
                     (2, 4, 8, 13, 100, None))

    queries += sweep("E", "tlp", "Reactions under the 415 top-level pathways",
                     lambda hops: f"(p:TopLevelPathway)-[:hasEvent{hops}]->(z:Reaction)",
                     "z",
                     (3, 6, 100, None))

    queries += sweep("E", "complex", "Sub-units of every complex",
                     lambda hops: f"(c:Complex)-[:hasComponent{hops}]->(z)",
                     "z",
                     (2, 100))

    queries += sweep("E", "reactions", "Reactions downstream of every reaction",
                     lambda hops: f"(r:Reaction)<-[:precedingEvent{hops}]-(z:Reaction)",
                     "z",
                     (3,),
                     (3, None))

    return tuple(queries)


def chainPattern(seed, hops, relationship, endLabel):
    pattern = seed

    for index in range(hops):
        label = endLabel if index == hops - 1 else ""
        pattern += f"-[{relationship}]->(n{index}{label})"

    return pattern


# Each expansion twice: as a quantified path, which walks trails, and as k relationship
# patterns, which walk anything - so the pair reads as the price of the quantifier.
def hopQueries():
    seed = f"(p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})"

    queries = hopSweep("A", "untyped", "Trails, any edge type",
                       lambda hops: f"MATCH {seed}-[e*{hops}..{hops}]->(m) RETURN count(m)")

    queries += hopSweep("B", "untyped_chain", "Chained patterns, any edge type",
                        lambda hops: f"MATCH {chainPattern(seed, hops, '', '')} RETURN count(n{hops - 1})")

    queries += hopSweep("C", "distinct", "Distinct ends of the trails",
                        lambda hops: f"MATCH {seed}-[e*{hops}..{hops}]->(m) RETURN count(DISTINCT m)")

    queries += hopSweep("D", "distinct_chain", "Distinct ends of the chained patterns",
                        lambda hops: f"MATCH {chainPattern(seed, hops, '', '')} RETURN count(DISTINCT n{hops - 1})")

    queries += hopSweep("E", "hasEvent", "hasEvent trails",
                        lambda hops: f"MATCH {seed}-[e:hasEvent*{hops}..{hops}]->(a:Event) RETURN count(a)")

    queries += hopSweep("F", "hasEvent_chain", "Chained hasEvent patterns",
                        lambda hops: f"MATCH {chainPattern(seed, hops, ':hasEvent', ':Event')} RETURN count(n{hops - 1})")

    return tuple(queries)


def fraudQueries():
    queries = [
        Query("S", "seed", f"Find account {RING_SEED} by id",
              f"MATCH (a:Account {{account_id: {RING_SEED}}}) RETURN count(a)"),
    ]

    queries += sweep("A", "seed_out", f"Accounts downstream of account {RING_SEED}",
                     lambda hops: f"(a:Account {{account_id: {RING_SEED}}})-[t:TRANSFER{hops}]->(b)",
                     "b",
                     (1, 2, 3, 4, 6, 8, None))

    queries += sweep("B", "all_out", "Accounts downstream of every account",
                     lambda hops: f"(a:Account)-[t:TRANSFER{hops}]->(b)",
                     "b",
                     (1, 2, 3))

    for bound in (4, RING_DEPTH, None):
        queries.append(Query("C", f"ring_seed_{bound or 'inf'}",
                             f"Rings through account {RING_SEED}, {boundLabel(bound)}",
                             f"MATCH (a:Account {{account_id: {RING_SEED}}})-[t:TRANSFER{rangeLiteral(bound)}]->(a) RETURN count(a)"))

    queries.append(Query("C", "ring_all_3",
                         "Accounts on a ring of 3 transfers, whole graph",
                         "MATCH (a:Account)-[t:TRANSFER*1..3]->(a) RETURN count(DISTINCT a)"))

    for bound in (4, RING_DEPTH, None):
        queries.append(Query("D", f"fraud_ring_seed_{bound or 'inf'}",
                             f"Fraud-only rings through account {RING_SEED}, {boundLabel(bound)}",
                             f"MATCH (a:Account {{account_id: {RING_SEED}}})-[t:TRANSFER{rangeLiteral(bound)}]->(a) "
                             f"WHERE all(e IN t WHERE e.is_fraud = true) RETURN count(a)"))

    return tuple(queries)


REACTOME = Fixture("~/.turing-bench", "reactome", "bolt://127.0.0.1:7687", "~/lbbench/reactome.ladybug", "Node", "reactome")

WORKLOADS = {
    "reactome": Workload("reactome",
                         "2,978,202 nodes and 11,537,331 edges of Reactome pathways; typed, tree-shaped, small balls",
                         REACTOME,
                         {"S": "Seed lookup, no path - what every seeded query below pays first",
                          "A": "Downstream of Signal Transduction, one seed",
                          "B": "Upstream of one hub reaction, one seed",
                          "C": "Downstream of all 415 top-level pathways",
                          "D": "Untyped ball around the top-level pathways"},
                         reactomeQueries()),

    "reactome-paths": Workload("reactome-paths",
                               "the 71 questions of docs/path_bench.md on Reactome",
                               REACTOME,
                               {"A": "Property-seeded typed queries - what a curator actually writes",
                                "B": "Label-seeded typed traversal - the engine, with no scan in the way",
                                "C": "Untyped variable-length",
                                "D": "Typed ranges, unbounded walks and a join",
                                "E": "Search against walk, by hop bound - count(DISTINCT) searches, count() walks"},
                               pathBenchQueries()),

    "reactome-hops": Workload("reactome-hops",
                              f"one expansion from Signal Transduction {SIGNAL_TRANSDUCTION} at each exact hop count",
                              REACTOME,
                              {"A": "Every trail of exactly k hops, any edge type",
                               "B": "The same expansion written as k relationship patterns",
                               "C": "The nodes those trails end on",
                               "D": "The nodes those k patterns end on",
                               "E": "Every hasEvent trail of exactly k hops",
                               "F": "The same hasEvent expansion as k patterns"},
                              hopQueries()),

    "fraud": Workload("fraud",
                      "1,000,000 accounts and 9,000,550 transfers from gen-fraud-graph; one edge type, out-degree 9, balls that saturate",
                      Fixture("~/.turing-fraud", "fraud_1m", "bolt://127.0.0.1:7688",
                              "~/.ladybug-fraud/fraud_1m", "", "fraud_1m"),
                      {"S": "Seed lookup, no path - what every seeded query below pays first",
                       "A": "Downstream of one account",
                       "B": "Downstream of all 1,000,000 accounts",
                       "C": "Laundering rings, a walk back to the account it started from",
                       "D": "Fraud-only rings, a predicate on every hop"},
                      fraudQueries()),
}
