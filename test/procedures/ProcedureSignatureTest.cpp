#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "ProcedureTypeVector.h"

using namespace db;

// The rendering SHOW PROCEDURES lists a procedure in. It is reachable through the
// statement only for the shapes some registered procedure happens to have - no
// registered one takes an optional argument, so the `= null` suffix has no other
// cover - and both the pipeline and the MLIR path feed a reused string into it.
class ProcedureSignatureTest : public ::testing::Test {
protected:
    // The namespace both owns the procedure and is what gives it its qualified
    // name, which is the first thing the signature prints.
    Procedure* addProcedure(std::string_view name) {
        Procedure* const procedure = new Procedure(name);
        _namespace.addProcedure(procedure);

        return procedure;
    }

    ProcedureNamespace _namespace {"db"};
    std::string _signature;
};

TEST_F(ProcedureSignatureTest, rendersAProcedureThatTakesNothing) {
    Procedure* const procedure = addProcedure("edgeTypes");
    procedure->addReturnValue("id", ProcedureType::EDGE_TYPE_ID);
    procedure->addReturnValue("edgeType", ProcedureType::STRING_VIEW);

    procedure->buildSignature(_signature);

    EXPECT_EQ(_signature, "db.edgeTypes() :: (id :: INTEGER, edgeType :: STRING)");
}

TEST_F(ProcedureSignatureTest, rendersArgumentsBeforeReturnValues) {
    Procedure* const procedure = addProcedure("describeCommit");
    procedure->addArgument("commit", ProcedureType::STRING);
    procedure->addReturnValue("hash", ProcedureType::STRING);
    procedure->addReturnValue("nodeCount", ProcedureType::UINT_64);

    procedure->buildSignature(_signature);

    EXPECT_EQ(_signature,
              "db.describeCommit(commit :: STRING) :: (hash :: STRING, nodeCount :: INTEGER)");
}

TEST_F(ProcedureSignatureTest, marksAnOptionalArgumentAsNullDefaulted) {
    Procedure* const procedure = addProcedure("sample");
    procedure->addArgument("node", ProcedureType::NODE);
    procedure->addOptionalArgument("seed", ProcedureType::INT64);
    procedure->addReturnValue("neighbour", ProcedureType::NODE);

    procedure->buildSignature(_signature);

    EXPECT_EQ(_signature,
              "db.sample(node :: NODE, seed :: INTEGER = null) :: (neighbour :: NODE)");
}

TEST_F(ProcedureSignatureTest, rendersEmptyParenthesesForAProcedureThatReportsNothing) {
    Procedure* const procedure = addProcedure("compact");

    procedure->buildSignature(_signature);

    EXPECT_EQ(_signature, "db.compact() :: ()");
}

// Both callers render every procedure into one string they reuse across the loop,
// so a signature must replace what the string held rather than append to it.
TEST_F(ProcedureSignatureTest, replacesWhatTheGivenStringHeld) {
    Procedure* const procedure = addProcedure("labels");
    procedure->addReturnValue("label", ProcedureType::STRING_VIEW);

    _signature = "db.somethingRenderedEarlier() :: (x :: INTEGER)";
    procedure->buildSignature(_signature);

    EXPECT_EQ(_signature, "db.labels() :: (label :: STRING)");
}
