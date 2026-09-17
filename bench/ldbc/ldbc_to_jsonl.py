#!/usr/bin/env python3
"""Convert an LDBC SNB "vanilla" CSV data set into the JSONL graph format TuringDB imports.

The label and relationship-type mapping is the one the official Neo4j reference
implementation uses (cypher/scripts/headers.txt and import-to-neo4j.sh), so the
official queries run against the graph unchanged:

  Place    -> Place:City / Place:Country / Place:Continent   (from its `type` column)
  Organisation -> Organisation:Company / Organisation:University
  Post     -> Post:Message
  Comment  -> Comment:Message

LDBC entity ids are unique only within a type, and the JSONL importer needs one
integer id per node, so each node gets a synthetic id here and keeps its LDBC id
as the `id` property the queries match on.

TuringDB properties are scalars, so Person.email and Person.speaks stay the
';'-separated strings the CSV holds rather than becoming lists.
"""

import argparse
import json
import os
import sys

SEPARATOR = "|"


class NodeIDs:
    """Hands out one globally unique integer id per (entity type, LDBC id) pair."""

    def __init__(self):
        self._ids = {}
        self._next = 0

    def assign(self, entity, ldbcID):
        key = (entity, ldbcID)
        synthetic = self._next
        self._ids[key] = synthetic
        self._next += 1
        return synthetic

    def lookup(self, entity, ldbcID):
        return self._ids.get((entity, ldbcID))

    def count(self):
        return self._next


def readRows(path):
    """Yields the header's column names, then each row as a list of fields.

    Self-referencing edge tables repeat a column name (`Place.id|Place.id`), so rows
    stay positional lists rather than dicts keyed by a name that would collapse.
    """
    with open(path, "r", encoding="utf-8") as handle:
        header = handle.readline().rstrip("\n").split(SEPARATOR)
        yield header

        for line in handle:
            fields = line.rstrip("\n").split(SEPARATOR)
            if len(fields) != len(header):
                continue
            yield fields


def asInt(value):
    """Parses an integer, or returns None when the field is empty or not a number."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def scalar(value):
    """Keeps a CSV field as an int when it reads as one, otherwise as a string."""
    parsed = asInt(value)
    return parsed if parsed is not None else value


# Node tables: file -> (entity, base labels, label column, property columns)
NODE_TABLES = [
    ("static/place_0_0.csv", "Place", ["Place"], "type", ["name", "url"]),
    ("static/organisation_0_0.csv", "Organisation", ["Organisation"], "type", ["name", "url"]),
    ("static/tagclass_0_0.csv", "TagClass", ["TagClass"], None, ["name", "url"]),
    ("static/tag_0_0.csv", "Tag", ["Tag"], None, ["name", "url"]),
    ("dynamic/person_0_0.csv", "Person", ["Person"], None,
     ["firstName", "lastName", "gender", "birthday", "creationDate", "locationIP",
      "browserUsed", "language", "email"]),
    ("dynamic/forum_0_0.csv", "Forum", ["Forum"], None, ["title", "creationDate"]),
    ("dynamic/post_0_0.csv", "Post", ["Post", "Message"], None,
     ["imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length"]),
    ("dynamic/comment_0_0.csv", "Comment", ["Comment", "Message"], None,
     ["creationDate", "locationIP", "browserUsed", "content", "length"]),
]

# The label a Place / Organisation `type` column value maps to
SUBLABELS = {
    "city": "City",
    "country": "Country",
    "continent": "Continent",
    "company": "Company",
    "university": "University",
}

# Person columns the Neo4j reference implementation renames
PERSON_RENAMES = {"language": "speaks"}

# Edge tables: file -> (edge type, source entity, target entity, property columns)
EDGE_TABLES = [
    ("static/place_isPartOf_place_0_0.csv", "IS_PART_OF", "Place", "Place", []),
    ("static/tagclass_isSubclassOf_tagclass_0_0.csv", "IS_SUBCLASS_OF", "TagClass", "TagClass", []),
    ("static/organisation_isLocatedIn_place_0_0.csv", "IS_LOCATED_IN", "Organisation", "Place", []),
    ("static/tag_hasType_tagclass_0_0.csv", "HAS_TYPE", "Tag", "TagClass", []),
    ("dynamic/comment_hasCreator_person_0_0.csv", "HAS_CREATOR", "Comment", "Person", []),
    ("dynamic/comment_isLocatedIn_place_0_0.csv", "IS_LOCATED_IN", "Comment", "Place", []),
    ("dynamic/comment_replyOf_comment_0_0.csv", "REPLY_OF", "Comment", "Comment", []),
    ("dynamic/comment_replyOf_post_0_0.csv", "REPLY_OF", "Comment", "Post", []),
    ("dynamic/forum_containerOf_post_0_0.csv", "CONTAINER_OF", "Forum", "Post", []),
    ("dynamic/forum_hasMember_person_0_0.csv", "HAS_MEMBER", "Forum", "Person", ["joinDate"]),
    ("dynamic/forum_hasModerator_person_0_0.csv", "HAS_MODERATOR", "Forum", "Person", []),
    ("dynamic/forum_hasTag_tag_0_0.csv", "HAS_TAG", "Forum", "Tag", []),
    ("dynamic/person_hasInterest_tag_0_0.csv", "HAS_INTEREST", "Person", "Tag", []),
    ("dynamic/person_isLocatedIn_place_0_0.csv", "IS_LOCATED_IN", "Person", "Place", []),
    ("dynamic/person_knows_person_0_0.csv", "KNOWS", "Person", "Person", ["creationDate"]),
    ("dynamic/person_likes_comment_0_0.csv", "LIKES", "Person", "Comment", ["creationDate"]),
    ("dynamic/person_likes_post_0_0.csv", "LIKES", "Person", "Post", ["creationDate"]),
    ("dynamic/person_studyAt_organisation_0_0.csv", "STUDY_AT", "Person", "Organisation", ["classYear"]),
    ("dynamic/person_workAt_organisation_0_0.csv", "WORK_AT", "Person", "Organisation", ["workFrom"]),
    ("dynamic/comment_hasTag_tag_0_0.csv", "HAS_TAG", "Comment", "Tag", []),
    ("dynamic/post_hasCreator_person_0_0.csv", "HAS_CREATOR", "Post", "Person", []),
    ("dynamic/post_hasTag_tag_0_0.csv", "HAS_TAG", "Post", "Tag", []),
    ("dynamic/post_isLocatedIn_place_0_0.csv", "IS_LOCATED_IN", "Post", "Place", []),
]


def writeNodes(csvDir, out, nodeIDs):
    """Writes one JSONL node line per row of every node table, returning the count."""
    written = 0

    for relativePath, entity, baseLabels, labelColumn, properties in NODE_TABLES:
        path = os.path.join(csvDir, relativePath)
        if not os.path.exists(path):
            print(f"missing node table: {path}", file=sys.stderr)
            continue

        rows = readRows(path)
        columns = {name: index for index, name in enumerate(next(rows))}

        for row in rows:
            ldbcID = row[columns["id"]]
            labels = list(baseLabels)

            if labelColumn is not None:
                sublabel = SUBLABELS.get(row[columns[labelColumn]])
                if sublabel is not None:
                    labels.append(sublabel)

            values = {"id": scalar(ldbcID)}
            for column in properties:
                index = columns.get(column)
                if index is None or row[index] == "":
                    continue
                name = PERSON_RENAMES.get(column, column) if entity == "Person" else column
                values[name] = scalar(row[index])

            line = {
                "type": "node",
                "id": str(nodeIDs.assign(entity, ldbcID)),
                "labels": labels,
                "properties": values,
            }
            out.write(json.dumps(line) + "\n")
            written += 1

    return written


def writeEdges(csvDir, out, nodeIDs):
    """Writes one JSONL relationship line per row of every edge table, returning the count."""
    written = 0
    dangling = 0

    for relativePath, edgeType, sourceEntity, targetEntity, properties in EDGE_TABLES:
        path = os.path.join(csvDir, relativePath)
        if not os.path.exists(path):
            print(f"missing edge table: {path}", file=sys.stderr)
            continue

        rows = readRows(path)
        next(rows)

        for row in rows:
            source = nodeIDs.lookup(sourceEntity, row[0])
            target = nodeIDs.lookup(targetEntity, row[1])

            if source is None or target is None:
                dangling += 1
                continue

            values = {}
            for offset, column in enumerate(properties):
                raw = row[2 + offset]
                if raw != "":
                    values[column] = scalar(raw)

            line = {
                "type": "relationship",
                "id": str(written),
                "label": edgeType,
                "properties": values,
                "start": {"id": str(source)},
                "end": {"id": str(target)},
            }
            out.write(json.dumps(line) + "\n")
            written += 1

    if dangling:
        print(f"skipped {dangling} edges with an unresolved endpoint", file=sys.stderr)

    return written


def main():
    parser = argparse.ArgumentParser(description="Convert LDBC SNB CSVs to TuringDB JSONL")
    parser.add_argument("csvDir", help="directory holding the static/ and dynamic/ CSV folders")
    parser.add_argument("output", help="JSONL file to write")
    args = parser.parse_args()

    nodeIDs = NodeIDs()

    with open(args.output, "w", encoding="utf-8") as out:
        nodeCount = writeNodes(args.csvDir, out, nodeIDs)
        edgeCount = writeEdges(args.csvDir, out, nodeIDs)

    print(f"{nodeCount} nodes, {edgeCount} edges -> {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
