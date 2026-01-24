#include <iostream>

#include "CypherAST.h"
#include "CypherASTDumper.h"
#include "CypherParser.h"
#include "TuringTime.h"
#include "CompilerException.h"
#include "FileReader.h"
#include "procedures/ProcedureBlueprintMap.h"

using namespace db;

int main(int argc, char** argv) {
    auto procedures = ProcedureBlueprintMap::create();

    std::string queryStr;

    if (argc > 1 && strlen(argv[1]) > 0) {
        queryStr = argv[1];

    } else {
        fs::Path path(SAMPLE_DIR "/parser-queries.txt");
        fmt::print("Reading query from file: {}\n", path.get());
        fs::File file = fs::File::open(path).value();
        fs::FileReader reader;

        reader.setFile(&file);
        reader.read();

        auto it = reader.iterateBuffer();

        queryStr = it.get<char>(file.getInfo()._size);
    }

    CypherAST ast(*procedures, queryStr);

    {
        // Try but allow not implemented to test the parser
        CypherParser parser(&ast);

        try {
            auto t0 = Clock::now();
            parser.parse(queryStr);
            auto t1 = Clock::now();
            fmt::print("Full query parsed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const CompilerException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_SUCCESS;
        }
    }

    {
        // Disallow not implemented.
        // This should print the AST if parsing was successful
        CypherParser parser(&ast);

        try {
            const auto t0 = Clock::now();
            parser.parse(queryStr);
            const auto t1 = Clock::now();
            fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const CompilerException& e) {
            fmt::print("{}\n", e.what());
            return 0; // Do not return error, user might test not implemented features
        }

        CypherASTDumper dumper(parser.getAST());
        dumper.dump(std::cout);
    }

    return EXIT_SUCCESS;
}
