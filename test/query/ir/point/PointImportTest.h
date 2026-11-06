#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "FileUtils.h"
#include "StringRowSink.h"
#include "TuringTest.h"

namespace db {
class NLOutputSink;
class QueryInterpreterV3;
}

namespace turing::test {

class TuringTestEnv;

// Drives QueryInterpreterV3 over simpledb, for tests that import a file of points into a
// graph of its own and read that graph back
class PointImportTest : public TuringTest {
public:
    PointImportTest();
    ~PointImportTest() override;

    void initialize() override;

protected:
    const std::string _sessionGraph {"simpledb"};

    // The directory LOAD JSONL and LOAD PARQUET read a relative path from
    void getDataDir(FileUtils::Path& dataDir) const;
    void writeFixture(std::string_view fileName, std::string_view contents);
    void runQuery(std::string_view query, std::string_view graphName, db::NLOutputSink& sink);
    void load(std::string_view query);
    void runQueryExpectingError(std::string_view query, std::string_view reason);
    void expectSortedRows(std::string_view query,
                          std::string_view graphName,
                          const std::vector<StringRowSink::Row>& expected);

private:
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<db::QueryInterpreterV3> _interpreter;
};

}
