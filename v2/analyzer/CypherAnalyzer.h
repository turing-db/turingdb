#pragma once

#include <memory>

namespace db {

class CypherAST;
class SinglePartQuery;
class Match;
class Skip;
class Limit;
class Return;
class Pattern;
class PatternElement;
class NodePattern;
class EdgePattern;
class DeclContext;

class CypherAnalyzer {
public:
    CypherAnalyzer(std::unique_ptr<CypherAST> ast);
    ~CypherAnalyzer();

    CypherAnalyzer(const CypherAnalyzer&) = delete;
    CypherAnalyzer(CypherAnalyzer&&) = delete;
    CypherAnalyzer& operator=(const CypherAnalyzer&) = delete;
    CypherAnalyzer& operator=(CypherAnalyzer&&) = delete;

    void analyze();

private:
    std::unique_ptr<CypherAST> _ast;

    std::unique_ptr<DeclContext> _rootCtxt;;
    DeclContext* _ctxt {nullptr};

    // Query types
    void analyze(const SinglePartQuery& query);

    // Statemens
    void analyze(const Match& matchSt);
    void analyze(const Skip& skipSt);
    void analyze(const Limit& limitSt);
    void analyze(const Return& returnSt);

    // Pattern
    void analyze(const Pattern& pattern);
    void analyze(const PatternElement& element);
    void analyze(NodePattern& node);
    void analyze(EdgePattern& edge);
};

}
