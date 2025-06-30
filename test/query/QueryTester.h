#pragma once

#include <gtest/gtest.h>

#include "LocalMemory.h"
#include "QueryInterpreter.h"
#include "versioning/CommitBuilder.h"
#include "TuringDB.h"
#include "Panic.h"
#include "columns/Block.h"

namespace db {

#define COL_CASE(Type)                                                \
    case Type::staticKind(): {                                        \
        EXPECT_EQ(col->getKind(), expectedCol->getKind());            \
        const auto* c = static_cast<const Type*>(col);                \
        const auto* ec = static_cast<const Type*>(expectedCol.get()); \
        if (checkEquality) {                                          \
            EXPECT_EQ(c->getRaw(), ec->getRaw());                     \
        }                                                             \
        Column* copiedCol = new Type(*c);                             \
        std::unique_ptr<Column> uniquePtr {copiedCol};                \
        _outputColumns.push_back(std::move(uniquePtr));               \
    } break;

class QueryTester {
public:
    QueryTester(LocalMemory& mem, QueryInterpreter& interp, const std::string& graphName = "simple")
        : _mem(mem),
        _interp(interp),
        _graphName(graphName)
    {
    }

    QueryTester& query(const std::string& query) {
        _outputColumns.clear();
        _expectedColumns.clear();
        _query = query;
        _expectError = false;
        return *this;
    }

    template <typename T>
    QueryTester& expectConst(const T& expectedValue,
                             bool checkEquality = true) {
        Column* col = new ColumnConst<T>(expectedValue);
        _expectedColumns.push_back({std::unique_ptr<Column> {col}, checkEquality});
        return *this;
    }

    template <typename T>
    QueryTester& expectVector(std::initializer_list<T> expectedValues,
                              bool checkEquality = true) {
        Column* col = new ColumnVector<T>(expectedValues);
        _expectedColumns.push_back({std::unique_ptr<Column> {col}, checkEquality});
        return *this;
    }

    template <typename T>
    QueryTester& expectOptVector(std::initializer_list<std::optional<T>> expectedValues,
                                 bool checkEquality = true) {
        Column* col = new ColumnOptVector<T>(expectedValues);
        _expectedColumns.push_back({std::unique_ptr<Column> {col}, checkEquality});
        return *this;
    }

    QueryTester& expectError() {
        _expectError = true;
        return *this;
    }

    QueryTester& execute() {
        fmt::print("Testing query: {}\n", _query);
        if (_expectError) {
            const auto res = _interp.execute(
                _query,
                _graphName,
                &_mem,
                [this](const Block& block) { fmt::print("Testing query: {}\n", _query); },
                [](const QueryCommand* cmd) {},
                _commitHash,
                _changeID);
            EXPECT_FALSE(res);
            return *this;
        }

        const auto res = _interp.execute(
            _query,
            _graphName,
            &_mem,
            [this](const Block& block) {
            const size_t colCount = block.columns().size();

            EXPECT_EQ(_expectedColumns.size(), block.columns().size());

            for (size_t i = 0; i < colCount; i++) {
                const Column* col = block.columns()[i];
                const auto& [expectedCol, checkEquality]= _expectedColumns[i];

                switch (col->getKind()) {
                    COL_CASE(ColumnVector<EntityID>)
                    COL_CASE(ColumnVector<NodeID>)
                    COL_CASE(ColumnVector<EdgeID>)
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
                    COL_CASE(ColumnConst<EntityID>)
                    COL_CASE(ColumnConst<NodeID>)
                    COL_CASE(ColumnConst<EdgeID>)
                    COL_CASE(ColumnConst<types::UInt64::Primitive>)
                    COL_CASE(ColumnConst<types::Int64::Primitive>)
                    COL_CASE(ColumnConst<types::Double::Primitive>)
                    COL_CASE(ColumnConst<types::String::Primitive>)
                    COL_CASE(ColumnConst<types::Bool::Primitive>)
                    COL_CASE(ColumnVector<const CommitBuilder*>)
                    COL_CASE(ColumnVector<const Change*>)

                    default: {
                        panic("can not check result for column of kind {}", col->getKind());
                    }
                }
            } },
            [](const QueryCommand* cmd) {},
            _commitHash, _changeID);

        EXPECT_TRUE(res);

        return *this;
    }

    template <typename T>
    std::optional<const ColumnVector<T>*> outputColumnVector(size_t index) {
        if (_outputColumns.size() <= index) {
            return std::nullopt;
        }

        const auto& col = _outputColumns[index];
        if (col->getKind() != ColumnVector<T>::staticKind()) {
            return std::nullopt;
        }

        return static_cast<const ColumnVector<T>*>(col.get());
    }

    template <typename T>
    std::optional<const ColumnConst<T>*> outputColumnConst(size_t index) {
        if (_outputColumns.size() <= index) {
            return std::nullopt;
        }

        const auto& col = _outputColumns[index];
        if (col->getKind() != ColumnConst<T>::staticKind()) {
            return std::nullopt;
        }

        return static_cast<const ColumnConst<T>*>(col.get());
    }

    void setGraphName(const std::string& graphName) {
        _graphName = graphName;
    }

    void setCommitHash(const CommitHash& commitHash) {
        _commitHash = commitHash;
    }

    void setChangeID(const ChangeID& changeID) {
        _changeID = changeID;
    }

private:
    LocalMemory& _mem;
    QueryInterpreter& _interp;
    CommitHash _commitHash = CommitHash::head();
    ChangeID _changeID = ChangeID::head();
    std::string _graphName = "simple";
    std::string _query;
    std::vector<std::pair<std::unique_ptr<Column>, bool>> _expectedColumns;
    std::vector<std::unique_ptr<Column>> _outputColumns;
    bool _expectError = false;
};
}
