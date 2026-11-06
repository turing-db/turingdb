#include <gtest/gtest.h>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "ParameterBindingTest.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* outputAnIntegerList = R"mlir(
func.func @main(%ids: !db.column<!storage.list<i64>> {storage.name = "ids"}) {
  db.output(%ids) names ["ids"] : !db.column<!storage.list<i64>>
  return
}
)mlir";

}

class ListParameterShapeBindingTest : public ParameterBindingTest {
};

TEST_F(ListParameterShapeBindingTest, aListOfTheProgramsShapeBinds) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setInt64(2);

    expectRows(outputAnIntegerList, parameters, {{"1, 2"}});
}

TEST_F(ListParameterShapeBindingTest, aNullElementIsOneOfTheShape) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setNull();

    expectRowCount(outputAnIntegerList, parameters, 1);
}

TEST_F(ListParameterShapeBindingTest, aListOfAnotherShapeIsRefused) {
    ParameterMap strings;
    strings.insert("ids").setList().emplace_back().setString("a");
    expectError(outputAnIntegerList, &strings, "Parameter $ids holds a String where the program reads Int64");

    ParameterMap nested;
    nested.insert("ids").setList().emplace_back().setList().emplace_back().setInt64(1);
    expectError(outputAnIntegerList, &nested, "Parameter $ids holds a List where the program reads Int64");

    ParameterMap scalar;
    scalar.insert("ids").setInt64(1);
    expectError(outputAnIntegerList, &scalar, "Parameter $ids holds a Int64 where the program reads a list");
}
