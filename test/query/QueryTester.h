#pragma once

#include <gtest/gtest.h>

#include "LocalMemory.h"
#include "QueryInterpreter.h"
#include "TuringDB.h"
#include "Panic.h"
#include "columns/Block.h"

namespace db {


#define COL_CASE(Type)                                                \
    case Type::staticKind(): {                                        \
        EXPECT_EQ(col->getKind(), expectedCol->getKind());            \
        const auto* c = static_cast<const Type*>(col);                \
        const auto* ec = static_cast<const Type*>(expectedCol.get()); \
        EXPECT_EQ(c->getRaw(), ec->getRaw());                         \
    } break;

class QueryTester {
public:
    QueryTester(LocalMemory& mem, QueryInterpreter& interp)
        : _mem(mem),
        _interp(interp)
    {
    }

    QueryTester& query(const std::string& query) {
        _expectedColumns.clear();
        _query = query;
        _expectError = false;
        return *this;
    }

    template <typename T>
    QueryTester& expectVector(std::initializer_list<T> expectedValues) {
        Column* col = new ColumnVector<T>(expectedValues);
        _expectedColumns.push_back(std::unique_ptr<Column> {col});
        return *this;
    }

    template <typename T>
    QueryTester& expectOptVector(std::initializer_list<std::optional<T>> expectedValues) {
        Column* col = new ColumnOptVector<T>(expectedValues);
        _expectedColumns.push_back(std::unique_ptr<Column> {col});
        return *this;
    }

    QueryTester& expectError() {
        _expectError = true;
        return *this;
    }

    void execute() {
        if (_expectError) {
            const auto res = _interp.execute(_query, "", &_mem, [this](const Block& block) {
                fmt::print("Testing query: {}\n", _query);
            });
            EXPECT_FALSE(res);
            return;
        }

        fmt::print("Testing query: {}\n", _query);
        const auto res = _interp.execute(_query, "", &_mem, [this](const Block& block) {
            const size_t colCount = block.columns().size();

            EXPECT_EQ(_expectedColumns.size(), block.columns().size());

            for (size_t i = 0; i < colCount; i++) {
                const Column* col = block.columns()[i];
                const auto& expectedCol = _expectedColumns[i];
                switch (col->getKind()) {
                    COL_CASE(ColumnVector<EntityID>)
                    COL_CASE(ColumnVector<types::UInt64::Primitive>)
                    COL_CASE(ColumnVector<types::Int64::Primitive>)
                    COL_CASE(ColumnVector<types::Double::Primitive>)
                    COL_CASE(ColumnVector<types::String::Primitive>)
                    COL_CASE(ColumnVector<types::Bool::Primitive>)
                    COL_CASE(ColumnOptVector<types::UInt64::Primitive>)
                    COL_CASE(ColumnOptVector<types::Int64::Primitive>)
                    COL_CASE(ColumnOptVector<types::Double::Primitive>)
                    COL_CASE(ColumnOptVector<types::String::Primitive>)
                    COL_CASE(ColumnOptVector<types::Bool::Primitive>)
                    COL_CASE(ColumnVector<std::string>)

                    default: {
                        panic("can not check result for column of kind {}", col->getKind());
                    }
                }
            }
        });

        EXPECT_TRUE(res);
    }

private:
    LocalMemory& _mem;
    QueryInterpreter& _interp;
    std::string _query;
    std::vector<std::unique_ptr<Column>> _expectedColumns;
    bool _expectError = false;
};
}
