#include <gtest/gtest.h>

#include <stdlib.h>

#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An entity a subquery returns after a MATCH, a MERGE or a CREATE wrote or found it, read
// by what follows the CALL. Remy is in the graph and Nia and Kai are not. Zed is returned
// by no branch, so an OPTIONAL CALL pads it.
namespace {

enum class Form {
    PLAIN,
    WHEN,
    UNION,
};

enum class Entity {
    NODE,
    EDGE,
};

enum class Write {
    MATCH,
    MERGE,
    CREATE,
    IMPORTED_CREATE,
    IMPORTED_MERGE,
};

enum class Consumer {
    READ,
    SET,
    LABEL,
    GROUP,
    DISTINCT,
    DELETE,
    CREATE_EDGE,
    MERGE_ENDPOINT,
};

struct WrittenEntityCase {
    Form _form {Form::PLAIN};
    Entity _entity {Entity::NODE};
    Write _remyWrite {Write::MATCH};
    Write _otherWrite {Write::MATCH};
    bool _optional {false};
    Consumer _consumer {Consumer::READ};
};

constexpr std::string_view NAMES[] {"Remy", "Nia", "Kai", "Zed"};

bool isImported(Write write) {
    return write == Write::IMPORTED_CREATE || write == Write::IMPORTED_MERGE;
}

bool importsAnEntity(const WrittenEntityCase& testCase) {
    return isImported(testCase._remyWrite) || isImported(testCase._otherWrite);
}

// The write the branch taken for @param name runs, and whether it is Remy's branch
bool writeOfRow(const WrittenEntityCase& testCase, std::string_view name, Write& write, bool& remySide) {
    if (name == "Zed") {
        return false;
    }

    remySide = name == "Remy" && testCase._form != Form::PLAIN;
    write = name == "Remy" ? testCase._remyWrite : testCase._otherWrite;

    return true;
}

bool returnsAnEntity(const WrittenEntityCase& testCase, std::string_view name) {
    Write write {Write::MATCH};
    bool remySide {false};
    if (!writeOfRow(testCase, name, write, remySide)) {
        return false;
    }

    return write != Write::MATCH || name == "Remy";
}

bool rowIsEmitted(const WrittenEntityCase& testCase, std::string_view name) {
    return testCase._optional || returnsAnEntity(testCase, name);
}

std::string nodeName(const WrittenEntityCase& testCase, std::string_view name) {
    Write write {Write::MATCH};
    bool remySide {false};
    writeOfRow(testCase, name, write, remySide);

    std::string returnedName {name};
    if (isImported(write)) {
        returnedName += "2";
    }

    return returnedName;
}

std::string edgeType(const WrittenEntityCase& testCase, std::string_view name) {
    Write write {Write::MATCH};
    bool remySide {false};
    writeOfRow(testCase, name, write, remySide);

    const bool knowsWell = write == Write::MATCH || (write == Write::MERGE && remySide);

    return knowsWell ? "KNOWS_WELL" : "TAGGED";
}

// What the query creates of the kind of entity under test, the import ahead of the CALL included
size_t createdCount(const WrittenEntityCase& testCase) {
    size_t created = importsAnEntity(testCase) ? std::size(NAMES) : 0;

    for (const std::string_view name : NAMES) {
        Write write {Write::MATCH};
        bool remySide {false};
        if (!writeOfRow(testCase, name, write, remySide)) {
            continue;
        }

        const bool mergeCreates = testCase._entity == Entity::NODE ? name != "Remy" : !remySide;
        if (write == Write::CREATE || (write == Write::MERGE && mergeCreates)) {
            created++;
        }
    }

    return created;
}

size_t returnedCount(const WrittenEntityCase& testCase) {
    size_t returned = 0;
    for (const std::string_view name : NAMES) {
        if (returnsAnEntity(testCase, name)) {
            returned++;
        }
    }

    return returned;
}

size_t paddedCount(const WrittenEntityCase& testCase) {
    if (!testCase._optional) {
        return 0;
    }

    return std::size(NAMES) - returnedCount(testCase);
}

std::string_view writeClause(Entity entity, Write write, bool remySide) {
    if (isImported(write)) {
        return "RETURN k AS x";
    }

    if (entity == Entity::NODE) {
        switch (write) {
            case Write::MATCH:
                return "MATCH (n:Person {name: name}) RETURN n AS x";
            break;
            case Write::MERGE:
                return "MERGE (n:Person {name: name}) RETURN n AS x";
            break;
            default:
                return "CREATE (n:Person {name: name}) RETURN n AS x";
            break;
        }
    }

    switch (write) {
        case Write::MATCH:
            return "MATCH (:Person {name: name})-[e:KNOWS_WELL]->() RETURN e AS x";
        break;
        case Write::MERGE:
            if (remySide) {
                return "MATCH (a:Person {name: name}), (b:Person {name: 'Adam'}) "
                       "MERGE (a)-[e:KNOWS_WELL]->(b) RETURN e AS x";
            }

            return "MERGE (:Tag {name: name})-[e:TAGGED]->(:Tag {name: name + '!'}) RETURN e AS x";
        break;
        default:
            return "CREATE (:Tag {name: name})-[e:TAGGED]->(:Tag) RETURN e AS x";
        break;
    }
}

std::string_view importClause(Entity entity, Write write) {
    const bool merges = write == Write::IMPORTED_MERGE;

    if (entity == Entity::NODE) {
        return merges ? "MERGE (k:Person {name: name + '2'}) WITH name, k "
                      : "CREATE (k:Person {name: name + '2'}) WITH name, k ";
    }

    return merges ? "MERGE (:Tag {name: name})-[k:TAGGED]->(:Tag {name: name + '!'}) WITH name, k "
                  : "CREATE (:Tag {name: name})-[k:TAGGED]->(:Tag) WITH name, k ";
}

std::string_view consumerClause(Entity entity, Consumer consumer) {
    if (entity == Entity::EDGE) {
        switch (consumer) {
            case Consumer::SET:
                return "SET x.w = 7 RETURN name, x.w";
            break;
            case Consumer::GROUP:
                return "WITH x, count(*) AS c RETURN type(x), c";
            break;
            case Consumer::DELETE:
                return "DELETE x RETURN name";
            break;
            default:
                return "RETURN name, type(x)";
            break;
        }
    }

    switch (consumer) {
        case Consumer::READ:
            return "RETURN name, x.name";
        break;
        case Consumer::SET:
            return "SET x.age = 7 RETURN name, x.age";
        break;
        case Consumer::LABEL:
            return "RETURN name, x:Person";
        break;
        case Consumer::GROUP:
            return "WITH x, count(*) AS c RETURN x.name, c";
        break;
        case Consumer::DISTINCT:
            return "WITH DISTINCT x RETURN x.name";
        break;
        case Consumer::DELETE:
            return "DETACH DELETE x RETURN name";
        break;
        case Consumer::CREATE_EDGE:
            return "CREATE (x)-[:HOLDS]->(:Badge) RETURN name";
        break;
        case Consumer::MERGE_ENDPOINT:
            return "MERGE (x)-[:OWNS]->(:Car {owner: name}) RETURN name";
        break;
    }

    return "";
}

void buildQuery(const WrittenEntityCase& testCase, std::string& query) {
    const Entity entity = testCase._entity;
    const bool imports = importsAnEntity(testCase);
    const std::string_view scope = imports ? "name, k" : "name";

    query = "UNWIND ['Remy', 'Nia', 'Kai', 'Zed'] AS name ";

    if (imports) {
        const Write imported = isImported(testCase._remyWrite) ? testCase._remyWrite : testCase._otherWrite;
        query += importClause(entity, imported);
    }

    query += testCase._optional ? "OPTIONAL CALL (" : "CALL (";
    query += scope;
    query += ") { ";

    const std::string_view remyClause = writeClause(entity, testCase._remyWrite, true);
    const std::string_view otherClause = writeClause(entity, testCase._otherWrite, false);

    switch (testCase._form) {
        case Form::PLAIN:
            query += "WITH ";
            query += scope;
            query += " WHERE name <> 'Zed' ";
            query += otherClause;
        break;
        case Form::WHEN:
            query += "WHEN name = 'Remy' THEN { ";
            query += remyClause;
            query += " } WHEN name <> 'Zed' THEN { ";
            query += otherClause;
            query += " }";
        break;
        case Form::UNION:
            query += "WITH ";
            query += scope;
            query += " WHERE name = 'Remy' ";
            query += remyClause;
            query += " UNION WITH ";
            query += scope;
            query += " WHERE name IN ['Nia', 'Kai'] ";
            query += otherClause;
        break;
    }

    query += " } ";
    query += consumerClause(entity, testCase._consumer);
}

void buildExpectedRows(const WrittenEntityCase& testCase, Rows& expected) {
    const bool isNode = testCase._entity == Entity::NODE;
    const Consumer consumer = testCase._consumer;

    if (consumer == Consumer::GROUP || consumer == Consumer::DISTINCT) {
        for (const std::string_view name : NAMES) {
            if (!returnsAnEntity(testCase, name)) {
                continue;
            }

            const std::string key = isNode ? nodeName(testCase, name) : edgeType(testCase, name);
            if (consumer == Consumer::GROUP) {
                expected.push_back({key, "1"});
            } else {
                expected.push_back({key});
            }
        }

        const size_t padded = paddedCount(testCase);
        if (padded == 0) {
            return;
        }

        if (consumer == Consumer::GROUP) {
            expected.push_back({"null", std::to_string(padded)});
        } else {
            expected.push_back({"null"});
        }

        return;
    }

    for (const std::string_view name : NAMES) {
        if (!rowIsEmitted(testCase, name)) {
            continue;
        }

        const bool returned = returnsAnEntity(testCase, name);
        const std::string rowName {name};

        switch (consumer) {
            case Consumer::READ:
                if (isNode) {
                    expected.push_back({rowName, returned ? nodeName(testCase, name) : "null"});
                } else {
                    expected.push_back({rowName, returned ? edgeType(testCase, name) : "null"});
                }
            break;
            case Consumer::SET:
                expected.push_back({rowName, returned ? "7" : "null"});
            break;
            case Consumer::LABEL:
                expected.push_back({rowName, returned ? "true" : "false"});
            break;
            default:
                expected.push_back({rowName});
            break;
        }
    }
}

// The read that shows what the write left in the graph, with what it should return, or
// false when the rows the query returned already say it
bool buildVerification(const WrittenEntityCase& testCase,
                       size_t entityCount,
                       std::string& query,
                       Rows& expected) {
    const bool isNode = testCase._entity == Entity::NODE;

    switch (testCase._consumer) {
        case Consumer::SET:
            query = isNode ? "MATCH (p:Person) WHERE p.age = 7 RETURN p.name"
                           : "MATCH ()-[r]->() WHERE r.w = 7 RETURN type(r)";

            for (const std::string_view name : NAMES) {
                if (returnsAnEntity(testCase, name)) {
                    expected.push_back({isNode ? nodeName(testCase, name) : edgeType(testCase, name)});
                }
            }

            return true;
        break;
        case Consumer::DELETE: {
            query = isNode ? "MATCH (p:Person) RETURN count(p)" : "MATCH ()-[r]->() RETURN count(r)";

            const size_t remaining = entityCount + createdCount(testCase) - returnedCount(testCase);
            expected.push_back({std::to_string(remaining)});

            return true;
        }
        break;
        case Consumer::CREATE_EDGE:
            query = "MATCH (p:Person)-[:HOLDS]->(:Badge) RETURN p.name";

            for (const std::string_view name : NAMES) {
                if (returnsAnEntity(testCase, name)) {
                    expected.push_back({nodeName(testCase, name)});
                }
            }

            return true;
        break;
        case Consumer::MERGE_ENDPOINT:
            query = "MATCH (p:Person)-[:OWNS]->(c:Car) RETURN p.name, c.owner";

            for (const std::string_view name : NAMES) {
                if (returnsAnEntity(testCase, name)) {
                    expected.push_back({nodeName(testCase, name), std::string(name)});
                }
            }

            return true;
        break;
        default:
            return false;
        break;
    }
}

void addCases(Entity entity, bool optional, std::vector<WrittenEntityCase>& cases) {
    std::vector<Consumer> consumers;
    if (entity == Entity::NODE) {
        consumers = {Consumer::READ, Consumer::SET, Consumer::LABEL, Consumer::GROUP, Consumer::DISTINCT, Consumer::DELETE};

        // A padded row holds no node to write an edge from
        if (!optional) {
            consumers.push_back(Consumer::CREATE_EDGE);
            consumers.push_back(Consumer::MERGE_ENDPOINT);
        }
    } else {
        consumers = {Consumer::READ, Consumer::SET, Consumer::GROUP, Consumer::DELETE};
    }

    const Write allWrites[] {Write::MATCH, Write::MERGE, Write::CREATE, Write::IMPORTED_CREATE, Write::IMPORTED_MERGE};
    const Write remyWrites[] {Write::MATCH, Write::MERGE, Write::CREATE};
    const Write otherWrites[] {Write::MERGE, Write::CREATE, Write::IMPORTED_CREATE, Write::IMPORTED_MERGE};

    for (const Consumer consumer : consumers) {
        for (const Write write : allWrites) {
            cases.push_back({Form::PLAIN, entity, write, write, optional, consumer});
        }

        for (const Form form : {Form::WHEN, Form::UNION}) {
            for (const Write remyWrite : remyWrites) {
                for (const Write otherWrite : otherWrites) {
                    cases.push_back({form, entity, remyWrite, otherWrite, optional, consumer});
                }
            }
        }
    }
}

std::vector<WrittenEntityCase> allCases() {
    std::vector<WrittenEntityCase> cases;
    for (const Entity entity : {Entity::NODE, Entity::EDGE}) {
        for (const bool optional : {false, true}) {
            addCases(entity, optional, cases);
        }
    }

    return cases;
}

std::string_view formName(Form form) {
    switch (form) {
        case Form::PLAIN:
            return "Plain";
        break;
        case Form::WHEN:
            return "When";
        break;
        case Form::UNION:
            return "Union";
        break;
    }

    return "";
}

std::string_view writeName(Write write) {
    switch (write) {
        case Write::MATCH:
            return "Match";
        break;
        case Write::MERGE:
            return "Merge";
        break;
        case Write::CREATE:
            return "Create";
        break;
        case Write::IMPORTED_CREATE:
            return "ImportedCreate";
        break;
        case Write::IMPORTED_MERGE:
            return "ImportedMerge";
        break;
    }

    return "";
}

std::string_view consumerName(Consumer consumer) {
    switch (consumer) {
        case Consumer::READ:
            return "Read";
        break;
        case Consumer::SET:
            return "Set";
        break;
        case Consumer::LABEL:
            return "Label";
        break;
        case Consumer::GROUP:
            return "Group";
        break;
        case Consumer::DISTINCT:
            return "Distinct";
        break;
        case Consumer::DELETE:
            return "Delete";
        break;
        case Consumer::CREATE_EDGE:
            return "CreateEdge";
        break;
        case Consumer::MERGE_ENDPOINT:
            return "MergeEndpoint";
        break;
    }

    return "";
}

std::string caseName(const testing::TestParamInfo<WrittenEntityCase>& info) {
    const WrittenEntityCase& testCase = info.param;

    std::string name {formName(testCase._form)};
    name += testCase._entity == Entity::NODE ? "_Node_" : "_Edge_";
    name += writeName(testCase._remyWrite);
    name += "_";
    name += writeName(testCase._otherWrite);
    name += testCase._optional ? "_Optional_" : "_";
    name += consumerName(testCase._consumer);

    return name;
}

}

class SubqueryWrittenEntityMatrixTest : public WriteQueryTest,
                                        public testing::WithParamInterface<WrittenEntityCase> {
protected:
    size_t readCount(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        EXPECT_TRUE(status.isOk()) << status.getError();

        return std::stoull(sink.rows().front().front());
    }
};

TEST_P(SubqueryWrittenEntityMatrixTest, readsTheReturnedEntity) {
    const WrittenEntityCase& testCase = GetParam();

    const size_t entityCount = testCase._entity == Entity::NODE ? readCount("MATCH (p:Person) RETURN count(p)")
                                                                : readCount("MATCH ()-[r]->() RETURN count(r)");

    std::string query;
    buildQuery(testCase, query);

    Rows expected;
    buildExpectedRows(testCase, expected);

    expectWriteRows(query, expected);

    std::string verification;
    Rows verified;
    if (buildVerification(testCase, entityCount, verification, verified)) {
        expectRows(verification, verified);
    }
}

INSTANTIATE_TEST_SUITE_P(Matrix,
                         SubqueryWrittenEntityMatrixTest,
                         testing::ValuesIn(allCases()),
                         caseName);
