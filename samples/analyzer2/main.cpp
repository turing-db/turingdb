#include <stdlib.h>

#include "AnalyzeException.h"
#include "CypherParser.h"
#include "Time.h"
#include "ParserException.h"
#include "CypherAnalyzer.h"
#include "FileReader.h"

using namespace db;

void runAnalyzer2(const std::string& query);

int main(int argc, char** argv) {
    std::string queryStr;

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

    runAnalyzer2(queryStr);

    return EXIT_SUCCESS;
}

void runAnalyzer2(const std::string& query) {
    CypherParser parser;
    parser.allowNotImplemented(true);

    try {
        auto t0 = Clock::now();
        parser.parse(query);
        auto t1 = Clock::now();
        fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, t1));
    } catch (const ParserException& e) {
        fmt::print("{}\n", e.what());
    }

    CypherAnalyzer analyzer {parser.takeAST()};

    try {
        auto t0 = Clock::now();
        analyzer.analyze(*ast);
        auto t1 = Clock::now();
        fmt::print("Query analyzed in {} us\n", duration<Microseconds>(t0, t1));
    } catch (const AnalyzeException& e) {
        fmt::print("{}\n", e.what());
    }
}
