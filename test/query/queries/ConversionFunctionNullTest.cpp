#include <gtest/gtest.h>

#include <optional>
#include <string_view>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class ConversionFunctionNullTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;

    auto query(std::string_view cypher, auto callback) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        return _db->query(cypher, state);
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }

    template <typename T>
    static std::optional<T> readSingle(const Dataframe* df, std::string_view name) {
        NamedColumn* named = findColumn(df, name);
        EXPECT_TRUE(named) << "missing column: " << name;
        if (!named) {
            return std::nullopt;
        }

        if (const auto* constant = named->as<ColumnConst<std::optional<T>>>()) {
            return constant->at(0);
        }
        if (const auto* vector = named->as<ColumnOptVector<T>>()) {
            EXPECT_EQ(vector->size(), 1u);
            return vector->at(0);
        }

        ADD_FAILURE() << "column has an unexpected type: " << name;
        return std::nullopt;
    }
};

TEST_F(ConversionFunctionNullTest, toIntegerReturnsNullOnUnparseableInput) {
    using Int = types::Int64::Primitive;

    std::optional<Int> valid;
    std::optional<Int> partial;
    std::optional<Int> decimalStr;
    std::optional<Int> emptyStr;
    std::optional<Int> overflowStr;

    const auto res = query(
        "RETURN toInteger('42') AS valid, "
        "toInteger('12notdigits') AS partial, "
        "toInteger('3.5') AS decimalStr, "
        "toInteger('') AS emptyStr, "
        "toInteger('99999999999999999999999') AS overflowStr",
        [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            valid = readSingle<Int>(df, "valid");
            partial = readSingle<Int>(df, "partial");
            decimalStr = readSingle<Int>(df, "decimalStr");
            emptyStr = readSingle<Int>(df, "emptyStr");
            overflowStr = readSingle<Int>(df, "overflowStr");
        });
    ASSERT_TRUE(res) << res.getError();

    ASSERT_TRUE(valid.has_value());
    EXPECT_EQ(*valid, 42);

    EXPECT_FALSE(partial.has_value());
    EXPECT_FALSE(decimalStr.has_value());
    EXPECT_FALSE(emptyStr.has_value());
    EXPECT_FALSE(overflowStr.has_value());
}

TEST_F(ConversionFunctionNullTest, toFloatReturnsNullOnUnparseableInput) {
    using Double = types::Double::Primitive;

    std::optional<Double> valid;
    std::optional<Double> partial;
    std::optional<Double> emptyStr;
    std::optional<Double> overflowStr;

    const auto res = query(
        "RETURN toFloat('2.5') AS valid, "
        "toFloat('2.5abc') AS partial, "
        "toFloat('') AS emptyStr, "
        "toFloat('1e400') AS overflowStr",
        [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            valid = readSingle<Double>(df, "valid");
            partial = readSingle<Double>(df, "partial");
            emptyStr = readSingle<Double>(df, "emptyStr");
            overflowStr = readSingle<Double>(df, "overflowStr");
        });
    ASSERT_TRUE(res) << res.getError();

    ASSERT_TRUE(valid.has_value());
    EXPECT_DOUBLE_EQ(*valid, 2.5);

    EXPECT_FALSE(partial.has_value());
    EXPECT_FALSE(emptyStr.has_value());
    EXPECT_FALSE(overflowStr.has_value());
}

TEST_F(ConversionFunctionNullTest, toBooleanReturnsNullOnUnparseableInput) {
    using Bool = types::Bool::Primitive;

    std::optional<Bool> trueVal;
    std::optional<Bool> falseVal;
    std::optional<Bool> invalid;
    std::optional<Bool> emptyStr;

    const auto res = query(
        "RETURN toBoolean('TRUE') AS trueVal, "
        "toBoolean('false') AS falseVal, "
        "toBoolean('maybe') AS invalid, "
        "toBoolean('') AS emptyStr",
        [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            trueVal = readSingle<Bool>(df, "trueVal");
            falseVal = readSingle<Bool>(df, "falseVal");
            invalid = readSingle<Bool>(df, "invalid");
            emptyStr = readSingle<Bool>(df, "emptyStr");
        });
    ASSERT_TRUE(res) << res.getError();

    ASSERT_TRUE(trueVal.has_value());
    EXPECT_TRUE(static_cast<bool>(*trueVal));
    ASSERT_TRUE(falseVal.has_value());
    EXPECT_FALSE(static_cast<bool>(*falseVal));

    EXPECT_FALSE(invalid.has_value());
    EXPECT_FALSE(emptyStr.has_value());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
