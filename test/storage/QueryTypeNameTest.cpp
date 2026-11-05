#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <optional>
#include <utility>

#include "QueryTypeName.h"
#include "decl/EvaluatedType.h"

using namespace db;

namespace {

struct CellTagType {
    CellTag _tag {CellTag::INVALID};
    EvaluatedType _type {EvaluatedType::Invalid};
};

constexpr std::array CELL_TAG_TYPES {
    CellTagType {CellTag::Int, EvaluatedType::Integer},
    CellTagType {CellTag::UInt, EvaluatedType::Integer},
    CellTagType {CellTag::Double, EvaluatedType::Double},
    CellTagType {CellTag::Bool, EvaluatedType::Bool},
    CellTagType {CellTag::String, EvaluatedType::String},
    CellTagType {CellTag::Embedding, EvaluatedType::Embedding},
    CellTagType {CellTag::ListView, EvaluatedType::List},
    CellTagType {CellTag::Null, EvaluatedType::Null},
    CellTagType {CellTag::NodeID, EvaluatedType::NodePattern},
    CellTagType {CellTag::EdgeID, EvaluatedType::EdgePattern},
    CellTagType {CellTag::DateTime, EvaluatedType::DateTime},
    CellTagType {CellTag::MapView, EvaluatedType::Map},
    CellTagType {CellTag::Duration, EvaluatedType::Duration},
    CellTagType {CellTag::Path, EvaluatedType::GraphPath},
    CellTagType {CellTag::Point, EvaluatedType::Point},
    CellTagType {CellTag::INVALID, EvaluatedType::Invalid},
};

static_assert(CELL_TAG_TYPES.size() == static_cast<size_t>(std::to_underlying(CellTag::INVALID)) + 1,
              "A CellTag was added: give it the type the analyzer names it by");

}

TEST(QueryTypeNameTest, namesEveryCellTagAsTheAnalyzerNamesItsType) {
    for (size_t ordinal = 0; ordinal < CELL_TAG_TYPES.size(); ordinal++) {
        const CellTag tag = static_cast<CellTag>(ordinal);
        const CellTagType& expected = CELL_TAG_TYPES[ordinal];

        ASSERT_EQ(expected._tag, tag);
        EXPECT_EQ(queryTypeName(tag), EvaluatedTypeName::value(expected._type)) << "tag " << ordinal;
    }
}

TEST(QueryTypeNameTest, namesEveryValueTypeAsTheAnalyzerNamesItsType) {
    for (size_t ordinal = 0; ordinal < static_cast<size_t>(std::to_underlying(ValueType::_SIZE)); ordinal++) {
        const ValueType type = static_cast<ValueType>(ordinal);
        const std::optional<EvaluatedType> evaluated = toEvaluatedType(type);

        ASSERT_TRUE(evaluated.has_value()) << "value type " << ordinal;
        EXPECT_EQ(queryTypeName(type), EvaluatedTypeName::value(*evaluated)) << "value type " << ordinal;
    }
}

TEST(QueryTypeNameTest, namesBothIntegersAsOneType) {
    EXPECT_EQ(queryTypeName(CellTag::Int), "Integer");
    EXPECT_EQ(queryTypeName(CellTag::UInt), "Integer");
    EXPECT_EQ(queryTypeName(ValueType::Int64), "Integer");
    EXPECT_EQ(queryTypeName(ValueType::UInt64), "Integer");
}
