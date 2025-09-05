#include <iostream>

#include "CypherAST.h"
#include "CypherASTDumper.h"
#include "CypherParser.h"
#include "TuringTime.h"
#include "ParserException.h"
#include "FileReader.h"

using namespace db;
using namespace db::v2;

int main(int argc, char** argv) {
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

    CypherAST ast(queryStr);

    {
        // Try but allow not implemented to test the parser
        CypherParser parser(ast);
        parser.allowNotImplemented(true);

        try {
            auto t0 = Clock::now();
            parser.parse(queryStr);
            auto t1 = Clock::now();
            fmt::print("Full query parsed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const ParserException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_FAILURE; // Should always succeed unless there's a bug
        }
    }

    {
        // Disallow not implemented.
        // This should print the AST if parsing was successful
        CypherParser parser(ast);
        parser.allowNotImplemented(false);

        try {
            auto t0 = Clock::now();
            parser.parse(queryStr);
            auto t1 = Clock::now();
            fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const ParserException& e) {
            fmt::print("{}\n", e.what());
            return 0; // Do not return error, user might test not implemented features
        }

        CypherASTDumper dumper(parser.getAST());
        dumper.dump(std::cout);
    }


    return EXIT_SUCCESS;
}
