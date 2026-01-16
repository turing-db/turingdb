#pragma once

namespace db {

class CypherAST;
class DeclContext;

class QueryCommand {
public:
    friend CypherAST;

    enum class Kind {
        SINGLE_PART_QUERY = 0,
        LOAD_GRAPH_QUERY,
        LOAD_NEO4J_QUERY,
        CHANGE_QUERY,
        COMMIT_QUERY,
        LIST_GRAPH_QUERY,
        CREATE_GRAPH_QUERY,
        LOAD_GML_QUERY,
        S3_CONNECT_QUERY,
        S3_TRANSFER_QUERY,
        SHOW_PROCEDURES_QUERY,
    };

    virtual Kind getKind() const = 0;

    DeclContext* getDeclContext() const { return _declContext; }

protected:
    DeclContext* _declContext {nullptr};

    QueryCommand(DeclContext* declContext);
    virtual ~QueryCommand();
};

}
