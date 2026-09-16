#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

#include "ProcedureManager.h"

#include "StringRowSink.h"
#include "TuringTest.h"

namespace db {
class Graph;
class GraphView;
}

namespace turing::test {

using Rows = std::vector<StringRowSink::Row>;

// Compiles a Cypher query against the shared SimpleGraph and reports the db program, the
// nl program it lowers to, or the rows it runs to.
//
// SimpleGraph is 18 nodes, so the cost model leaves every cut a hash join test writes as
// the product it stands as. What the join does with a cut it takes is what those tests are
// about, so the pass context forces every match the pass finds unless a case asks for the
// form the cost model would have left.
class HashJoinQueryTest : public TuringTest {
public:
    HashJoinQueryTest();
    ~HashJoinQueryTest() override;

protected:
    void initialize() override;

    void dbProgram(std::string_view query, std::string& program);
    void nlProgram(std::string_view query, std::string& program);

    void runQuery(std::string_view query, StringRowSink& sink, bool forcesJoin = true);

    void expectCount(std::string_view query, size_t expected);
    void expectRows(std::string_view query, const Rows& expected);

    static bool contains(std::string_view text, std::string_view part);

    std::unique_ptr<db::Graph> _graph;
    db::ProcedureManager _procedures;
    mlir::MLIRContext _context;

    void generate(std::string_view query,
                  const db::GraphView& view,
                  mlir::ModuleOp module,
                  bool forcesJoin);

    void lower(const db::GraphView& view, mlir::ModuleOp module, mlir::ModuleOp nlModule);

    static void render(mlir::ModuleOp module, std::string& program);
};

}
