#include <gtest/gtest.h>

#include "Procedure.h"
#include "ProcedureException.h"
#include "ProcedureManager.h"
#include "ProcedureNamespace.h"
#include "ProcedureTypeVector.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

class ProcedureRegistrationTest : public TuringTest {
protected:
    void initialize() override {
        _testNamespace = _procedures.createNamespace("test");
    }

    ProcedureManager _procedures;
    ProcedureNamespace* _testNamespace {nullptr};
};

TEST_F(ProcedureRegistrationTest, rowAlignedArgumentWithoutIndicesIsRefused) {
    Procedure* procedure = new Procedure("perRow");
    procedure->addArgument("nodeIDs", ProcedureType::NODE);
    procedure->addReturnValue("value", ProcedureType::INT64);

    EXPECT_THROW(_testNamespace->addProcedure(procedure), ProcedureException);
    EXPECT_EQ(_testNamespace->getProcedure("perRow"), nullptr);
}

TEST_F(ProcedureRegistrationTest, rowAlignedArgumentReportingIndicesIsAccepted) {
    Procedure* procedure = new Procedure("perRow");
    procedure->setHasIndices(true);
    procedure->addArgument("nodeIDs", ProcedureType::NODE);
    procedure->addReturnValue("value", ProcedureType::INT64);

    _testNamespace->addProcedure(procedure);

    EXPECT_EQ(_testNamespace->getProcedure("perRow"), procedure);
}

TEST_F(ProcedureRegistrationTest, constantArgumentsWithoutIndicesAreAccepted) {
    Procedure* procedure = new Procedure("configured");
    procedure->addConstantArgument("nodeIDs", ProcedureType::LIST);
    procedure->addOptionalConstantArgument("seed", ProcedureType::INT64);
    procedure->addReturnValue("value", ProcedureType::INT64);

    _testNamespace->addProcedure(procedure);

    EXPECT_EQ(_testNamespace->getProcedure("configured"), procedure);
    EXPECT_FALSE(procedure->hasRowAlignedArgument());
}

TEST_F(ProcedureRegistrationTest, argumentLessProcedureWithoutIndicesIsAccepted) {
    Procedure* procedure = new Procedure("source");
    procedure->addReturnValue("value", ProcedureType::INT64);

    _testNamespace->addProcedure(procedure);

    EXPECT_EQ(_testNamespace->getProcedure("source"), procedure);
    EXPECT_FALSE(procedure->hasRowAlignedArgument());
}

TEST_F(ProcedureRegistrationTest, oneRowAlignedArgumentAmongConstantsStillNeedsIndices) {
    Procedure* procedure = new Procedure("sampled");
    procedure->addArgument("node", ProcedureType::NODE);
    procedure->addConstantArgument("sampleSize", ProcedureType::INT64);
    procedure->addReturnValue("tgt", ProcedureType::NODE);

    EXPECT_TRUE(procedure->hasRowAlignedArgument());
    EXPECT_THROW(_testNamespace->addProcedure(procedure), ProcedureException);
}

TEST_F(ProcedureRegistrationTest, everyRegisteredProcedureObeysTheContract) {
    ProcedureManager registry;
    EXPECT_NO_THROW(registry.init());
}
