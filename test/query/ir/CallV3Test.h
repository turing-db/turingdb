#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "QueryConfig.h"
#include "versioning/ChangeID.h"

#include "TuringTest.h"

namespace db {
class NLOutputSink;
class QueryInterpreterV3;
}

namespace turing::test {

class TuringTestEnv;

// Drives the whole engine through QueryInterpreterV3 on the simpledb fixture: reads run on
// the head, writes run in a change of their own that is then submitted, so a read that
// follows sees what they wrote.
class CallV3Test : public TuringTest {
public:
    CallV3Test();
    ~CallV3Test() override;

    void initialize() override;

protected:
    void runQuery(std::string_view query, db::NLOutputSink& sink);
    void runQueryExpectingError(std::string_view query);
    void runWrite(std::string_view query);
    void runWriteExpectingError(std::string_view query, std::string_view reason);
    void runLegacyWrite(std::string_view query);

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<db::QueryInterpreterV3> _interpreter;
    db::QueryConfig _queryConfig;

    void newChange(db::ChangeID& changeID);
    void submitChange(db::ChangeID changeID);
};

}
