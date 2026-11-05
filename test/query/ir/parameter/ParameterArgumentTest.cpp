#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"

#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageAttributes.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "ParameterMap.h"
#include "ParameterValue.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// main's arguments as "name: type", one per line
std::string printArguments(mlir::func::FuncOp function) {
    std::string printed;
    llvm::raw_string_ostream stream(printed);

    for (const mlir::BlockArgument argument : function.getArguments()) {
        const mlir::StringAttr name = function.getArgAttrOfType<mlir::StringAttr>(argument.getArgNumber(),
                                                                                  mlir::storage::PARAMETER_NAME_ATTRIBUTE);
        stream << (name ? name.getValue() : "<no name>") << ": ";
        argument.getType().print(stream);
        stream << "\n";
    }

    return printed;
}

}

class ParameterArgumentTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);

        ParameterValue min;
        min.setInt64(1);
        _parameters.set("min", min);

        ParameterValue ratio;
        ratio.setDouble(1.5);
        _parameters.set("ratio", ratio);

        ParameterValue name;
        name.setString("a");
        _parameters.set("name", name);

        ParameterValue open;
        open.setBool(true);
        _parameters.set("open", open);

        ParameterValue ids;
        ids.setList().emplace_back().setInt64(1);
        _parameters.set("ids", ids);

        ParameterValue names;
        ParameterValue::List& strings = names.setList();
        strings.emplace_back().setString("a");
        strings.emplace_back().setString("b");
        _parameters.set("names", names);

        ParameterValue rows;
        ParameterValue::List& lists = rows.setList();
        lists.emplace_back().setList().emplace_back().setInt64(1);
        lists.emplace_back().setList().emplace_back().setInt64(2);
        _parameters.set("rows", rows);

        ParameterValue mixed;
        ParameterValue::List& kinds = mixed.setList();
        kinds.emplace_back().setInt64(1);
        kinds.emplace_back().setString("a");
        _parameters.set("mixed", mixed);

        ParameterValue empty;
        empty.setList();
        _parameters.set("empty", empty);

        ParameterValue nothing;
        nothing.setNull();
        _parameters.set("nothing", nothing);
    }

    // The arguments of main in the db program of @param query, then in its nl lowering
    void compile(std::string_view query, std::string& dbArguments, std::string& nlArguments) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(system.getProcedures(), query);
        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view, &_parameters);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);
        ASSERT_TRUE(mlir::succeeded(mlir::verify(moduleOp)));

        const mlir::func::FuncOp dbFunction = moduleOp.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);
        dbArguments = printArguments(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*nlModule)));

        const mlir::func::FuncOp nlFunction = nlModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(nlFunction);
        nlArguments = printArguments(nlFunction);
    }

    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    ParameterMap _parameters;
};

TEST_F(ParameterArgumentTest, aParameterIsANullableArgumentOfTheTypeOfItsValue) {
    std::string dbArguments;
    std::string nlArguments;
    compile("RETURN $min + 1, $ratio * 2.0, $name + 'a', $open AND true", dbArguments, nlArguments);

    EXPECT_EQ(dbArguments,
              "min: !db.column<i64>\n"
              "ratio: !db.column<f64>\n"
              "name: !db.column<!storage.string>\n"
              "open: !db.column<i1>\n");
    EXPECT_EQ(nlArguments,
              "min: !nl.chunk<i64>\n"
              "ratio: !nl.chunk<f64>\n"
              "name: !nl.chunk<!storage.string>\n"
              "open: !nl.chunk<i1>\n");
}

TEST_F(ParameterArgumentTest, theOccurrencesOfAParameterReadOneArgument) {
    std::string dbArguments;
    std::string nlArguments;
    compile("MATCH (n:Person) WHERE n.age > $min AND n.age < $min + 10 RETURN n.name, $min", dbArguments, nlArguments);

    EXPECT_EQ(dbArguments, "min: !db.column<i64>\n");
    EXPECT_EQ(nlArguments, "min: !nl.chunk<i64>\n");
}

TEST_F(ParameterArgumentTest, aNullIsANullArgument) {
    std::string dbArguments;
    std::string nlArguments;
    compile("RETURN $nothing", dbArguments, nlArguments);

    EXPECT_EQ(dbArguments, "nothing: !db.column<!storage.nullable<none>>\n");
    EXPECT_EQ(nlArguments, "nothing: !nl.chunk<!storage.nullable<none>>\n");
}

TEST_F(ParameterArgumentTest, aQueryWithoutParametersTakesNoArgument) {
    std::string dbArguments;
    std::string nlArguments;
    compile("MATCH (n:Person) RETURN n.name", dbArguments, nlArguments);

    EXPECT_EQ(dbArguments, "");
    EXPECT_EQ(nlArguments, "");
}

TEST_F(ParameterArgumentTest, aLabelParameterIsNoArgument) {
    std::string dbArguments;
    std::string nlArguments;
    compile("MATCH (n:$name) RETURN n", dbArguments, nlArguments);

    EXPECT_EQ(dbArguments, "");
    EXPECT_EQ(nlArguments, "");
}

TEST_F(ParameterArgumentTest, aListParameterIsTypedByItsShape) {
    std::string dbArguments;
    std::string nlArguments;
    compile("RETURN $ids, $names, $rows, $mixed, $empty", dbArguments, nlArguments);

    EXPECT_EQ(dbArguments,
              "ids: !db.column<!storage.list<i64>>\n"
              "names: !db.column<!storage.list<!storage.string>>\n"
              "rows: !db.column<!storage.list<!storage.list<i64>>>\n"
              "mixed: !db.column<!storage.list<!storage.any>>\n"
              "empty: !db.column<!storage.list<none>>\n");
    EXPECT_EQ(nlArguments,
              "ids: !nl.chunk<!storage.list<i64>>\n"
              "names: !nl.chunk<!storage.list<!storage.string>>\n"
              "rows: !nl.chunk<!storage.list<!storage.list<i64>>>\n"
              "mixed: !nl.chunk<!storage.list<!storage.any>>\n"
              "empty: !nl.chunk<!storage.list<none>>\n");
}
