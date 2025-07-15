#include <stdlib.h>

#include <sstream>

#include "Time.h"
#include "YCypherScanner.h"
#include "ParserException.h"
#include "FileReader.h"
#include "CypherAST.h"

using namespace db;

void runParser2(const std::string& query);

int main(int argc, char** argv) {
    std::string queryStr;

    if (argc == 2) {
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

    runParser2(queryStr);

    return EXIT_SUCCESS;
}

void runParser2(const std::string& query) {
    YCypherScanner yscanner;
    CypherAST ast;
    yscanner.setThrowNotImplemented(true);
    yscanner.setQuery(query);

    YCypherParser yparser(yscanner, ast);

    std::istringstream iss;
    iss.rdbuf()->pubsetbuf((char*)query.data(), query.size());

    yscanner.switch_streams(&iss, NULL);

    try {
        auto t0 = Clock::now();
        yparser.parse();
        fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, Clock::now()));
    } catch (const ParserException& e) {
        fmt::print("{}\n", e.what());
    }
}
