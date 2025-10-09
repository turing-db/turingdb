#include <iostream>
#include <memory>
#include <regex>

#include <tabulate/table.hpp>

#include "CypherASTDumper.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "PlannerException.h"
#include "TuringTime.h"
#include "ParserException.h"
#include "FileReader.h"
#include "SimpleGraph.h"
#include "CypherAST.h"
#include "AnalyzeException.h"
#include "versioning/Transaction.h"
#include "columns/Block.h"
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "PlanGraphDebug.h"
#include "PipelineGenerator.h"
#include "PipelineV2.h"
#include "LocalMemory.h"
#include "PipelineDebug.h"
#include "ExecutionContext.h"
#include "PipelineExecutor.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"

#include "Panic.h"

using namespace db;
using namespace db::v2;

// ====== Tabulate Utils ======
template <typename T>
void tabulateWrite(tabulate::RowStream& rs, const T& value) {
    // @_ref HistoryStep uses double escaped new line (\\n) so that it is valid JSON
    // if the `/query -d "history"` endpoint is hit. When writing to CLI we replace double
    // escaped with single escape so that it is rendered in terminal correctly.
    if constexpr (std::same_as<T, std::string>) {
        std::regex re(R"(\\n)");  
        rs << std::regex_replace(value, re, "\n");
        return;
    }
    rs << value;
}

template <typename T>
void tabulateWrite(tabulate::RowStream& rs, const std::optional<T>& value) {
    if (value) {
        rs << *value;
    } else {
        rs << "null";
    }
}

void tabulateWrite(tabulate::RowStream& rs, const CommitBuilder* commit) {
    rs << fmt::format("{:x}", commit->hash().get());
}

void tabulateWrite(tabulate::RowStream& rs, const Change* change) {
    rs << fmt::format("{:x}", change->id().get());
}

#define TABULATE_COL_CASE(Type, i)                        \
    case Type::staticKind(): {                            \
        const Type& src = *static_cast<const Type*>(col); \
        tabulateWrite(rs, src[i]);                        \
    } break;

#define TABULATE_COL_CONST_CASE(Type)                     \
    case Type::staticKind(): {                            \
        const Type& src = *static_cast<const Type*>(col); \
        tabulateWrite(rs, src.getRaw());                  \
    } break;

// ====== End Tabulate Utils ======


int main(int argc, char** argv) {
    std::string queryStr;

    std::unique_ptr<Graph> graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const Transaction transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    if (argc > 1 && strlen(argv[1]) > 0) {
        queryStr = argv[1];

    } else {
        fs::Path path(SAMPLE_DIR "/queries.txt");
        fmt::print("Reading query from file: {}\n", path.get());
        fs::File file = fs::File::open(path).value();
        fs::FileReader reader;

        reader.setFile(&file);
        reader.read();

        auto it = reader.iterateBuffer();

        queryStr = it.get<char>(file.getInfo()._size);
    }

    tabulate::Table table;

    auto queryCallback = [&table](const Block& block) {
        const size_t rowCount = block.getBlockRowCount();

        for (size_t i = 0; i < rowCount; ++i) {
            tabulate::RowStream rs;
            for (const Column* col : block.columns()) {
                switch (col->getKind()) {
                    TABULATE_COL_CASE(ColumnVector<EntityID>, i)
                    TABULATE_COL_CASE(ColumnVector<NodeID>, i)
                    TABULATE_COL_CASE(ColumnVector<EdgeID>, i)
                    TABULATE_COL_CASE(ColumnVector<PropertyTypeID>, i)
                    TABULATE_COL_CASE(ColumnVector<LabelSetID>, i)
                    TABULATE_COL_CASE(ColumnVector<types::UInt64::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<types::Int64::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<types::Double::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<types::String::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<types::Bool::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::UInt64::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::Int64::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::Double::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::String::Primitive>, i)
                    TABULATE_COL_CASE(ColumnOptVector<types::Bool::Primitive>, i)
                    TABULATE_COL_CASE(ColumnVector<std::string>, i)
                    TABULATE_COL_CASE(ColumnVector<const CommitBuilder*>, i)
                    TABULATE_COL_CASE(ColumnVector<const Change*>, i)
                    TABULATE_COL_CONST_CASE(ColumnConst<EntityID>)
                    TABULATE_COL_CONST_CASE(ColumnConst<NodeID>)
                    TABULATE_COL_CONST_CASE(ColumnConst<EdgeID>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::UInt64::Primitive>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::Int64::Primitive>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::Double::Primitive>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::String::Primitive>)
                    TABULATE_COL_CONST_CASE(ColumnConst<types::Bool::Primitive>)

                    default: {
                        panic("can not print columns of kind {}", col->getKind());
                    }
                }
            }

            table.add_row(std::move(rs));
        }
    };

    CypherAST ast(queryStr);
    ast.setDebugLocations(true);

    fmt::print("\n========== Query Parsing ==========\n");
    {
        CypherParser parser(&ast);
        parser.allowNotImplemented(false);

        try {
            auto t0 = Clock::now();
            parser.parse(queryStr);
            auto t1 = Clock::now();
            fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const ParserException& e) {
            fmt::print("{}\n", e.what());
            return 0;
        }

        CypherAnalyzer analyzer(&ast, view);

        try {
            auto t0 = Clock::now();
            analyzer.analyze();
            auto t1 = Clock::now();
            fmt::print("Query analyzed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const AnalyzeException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_FAILURE;
        }


        CypherASTDumper dumper(analyzer.getAST());
        dumper.dump(std::cout);
    }

    fmt::print("\n========== Plan Graph Generation ==========\n");
    PlanGraphGenerator planGen(ast, view);
    PlanGraph& planGraph = planGen.getPlanGraph();
    {
        try {
            auto t0 = Clock::now();
            planGen.generate(ast.queries().front());
            auto t1 = Clock::now();
            fmt::print("Query plan generated in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const PlannerException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_FAILURE;
        }
        const PlanGraph& planGraph = planGen.getPlanGraph();

        PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);
    }

    PipelineV2 pipeline;
    LocalMemory localMem;

    fmt::print("\n========== Pipeline Generation ==========\n");
    {
        PipelineGenerator pipelineGenerator(&planGraph, &pipeline, &localMem, queryCallback);
        try {
            auto t0 = Clock::now();
            pipelineGenerator.generate();
            auto t1 = Clock::now();
            fmt::print("Pipeline generated in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const PlannerException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_FAILURE;
        }

        PipelineDebug::dumpMermaid(std::cout, &pipeline);
    }

    fmt::print("\n========== Pipeline Execution ==========\n");
    {
        auto t0 = Clock::now();
        ExecutionContext execCtxt(view);
        PipelineExecutor executor(&pipeline, &execCtxt);
        executor.execute();
        auto t1 = Clock::now();
        fmt::print("Pipeline executed in {} us\n", duration<Microseconds>(t0, t1));
    }

    std::cout << table << "\n";

    return EXIT_SUCCESS;
}
