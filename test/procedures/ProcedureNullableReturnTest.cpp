#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "Procedure.h"
#include "ProcedureException.h"
#include "ProcedureNamespace.h"
#include "ProcedureTypeVector.h"

using namespace db;

// A return value declared nullable, which is what makes the engine give the procedure a
// ColumnOptVector to write through rather than a ColumnVector.
class ProcedureNullableReturnTest : public ::testing::Test {
protected:
    Procedure* addProcedure(std::string_view name) {
        Procedure* const procedure = new Procedure(name);
        _namespace.addProcedure(procedure);

        return procedure;
    }

    ProcedureNamespace _namespace {"gnn"};
    std::string _signature;
};

TEST_F(ProcedureNullableReturnTest, declaresAPlainReturnValueAsNotNullable) {
    Procedure* const procedure = addProcedure("sample");
    procedure->addReturnValue("src", ProcedureType::NODE);

    EXPECT_FALSE(procedure->isReturnValueNullable(procedure->getReturnValueIndex("src")));
}

TEST_F(ProcedureNullableReturnTest, declaresANullableReturnValue) {
    Procedure* const procedure = addProcedure("sample");
    procedure->addNullableReturnValue("score", ProcedureType::INT64);

    EXPECT_TRUE(procedure->isReturnValueNullable(procedure->getReturnValueIndex("score")));
}

// A node or edge column holds its own null as an invalid ID, so neither is declared nullable
TEST_F(ProcedureNullableReturnTest, rejectsANullableNodeOrEdge) {
    Procedure* const procedure = addProcedure("sample");

    EXPECT_THROW(procedure->addNullableReturnValue("tgt", ProcedureType::NODE), ProcedureException);
    EXPECT_THROW(procedure->addNullableReturnValue("edge", ProcedureType::EDGE), ProcedureException);
}

TEST_F(ProcedureNullableReturnTest, keepsTheDeclarationOrderAcrossBothForms) {
    Procedure* const procedure = addProcedure("sample");
    procedure->addReturnValue("src", ProcedureType::NODE);
    procedure->addNullableReturnValue("label", ProcedureType::LABEL_ID);
    procedure->addReturnValue("hops", ProcedureType::INT64);

    EXPECT_EQ(procedure->getReturnValueIndex("src"), 0u);
    EXPECT_EQ(procedure->getReturnValueIndex("label"), 1u);
    EXPECT_EQ(procedure->getReturnValueIndex("hops"), 2u);

    EXPECT_FALSE(procedure->isReturnValueNullable(0));
    EXPECT_TRUE(procedure->isReturnValueNullable(1));
    EXPECT_FALSE(procedure->isReturnValueNullable(2));
}

// Nullability is a property of the column, not of the type the signature names, so
// SHOW PROCEDURES lists a nullable INTEGER as an INTEGER.
TEST_F(ProcedureNullableReturnTest, rendersANullableReturnValueAsItsType) {
    Procedure* const procedure = addProcedure("sample");
    procedure->addArgument("node", ProcedureType::NODE);
    procedure->addNullableReturnValue("score", ProcedureType::INT64);

    procedure->buildSignature(_signature);

    EXPECT_EQ(_signature, "gnn.sample(node :: NODE) :: (score :: INTEGER)");
}
