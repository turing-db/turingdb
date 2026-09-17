"""
The graphs the benchmark asks its questions of, and the questions themselves.

Each workload sweeps one pattern over a list of hop bounds, unbounded included, in two
modes: the walk counts every trail the pattern matches, the search counts the distinct
nodes it ends on. Every query is plain openCypher - a range literal on the relationship,
`all()` for a predicate on every hop - and is sent to each database verbatim, except
ladybug, whose dialect the client translates into.

A fixture says where each database keeps the same graph: a turing dir and graph name, a
bolt uri, a ladybug database directory, and a falkor graph key. `ladybugNodeTable` names
the one table the fixture packs every label into, empty where a label is its own table.
"""

from dataclasses import dataclass

SIGNAL_TRANSDUCTION = "R-HSA-162582"
HUB_REACTION = "R-HSA-2993780"

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


# One pattern over every bound, counted as trails and as distinct ends. `pattern` takes
# the range literal to write inside the relationship brackets.
def sweep(group, prefix, question, pattern, end, bounds):
    queries = []

    for mode in ("walk", "search"):
        aggregate = f"count(DISTINCT {end})" if mode == "search" else f"count({end})"

        for bound in bounds:
            queries.append(Query(group,
                                 f"{prefix}_{mode}_{bound or 'inf'}",
                                 f"{question}, {boundLabel(bound)}, {mode}",
                                 f"MATCH {pattern(rangeLiteral(bound))} RETURN {aggregate}"))

    return queries


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


WORKLOADS = {
    "reactome": Workload("reactome",
                         "2,978,202 nodes and 11,537,331 edges of Reactome pathways; typed, tree-shaped, small balls",
                         Fixture("~/.turing-bench", "reactome", "bolt://127.0.0.1:7687",
                                 "~/lbbench/reactome.ladybug", "Node", "reactome"),
                         {"S": "Seed lookup, no path - what every seeded query below pays first",
                          "A": "Downstream of Signal Transduction, one seed",
                          "B": "Upstream of one hub reaction, one seed",
                          "C": "Downstream of all 415 top-level pathways",
                          "D": "Untyped ball around the top-level pathways"},
                         reactomeQueries()),

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
