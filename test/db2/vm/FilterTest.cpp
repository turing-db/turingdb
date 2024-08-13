#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <range/v3/view/zip.hpp>

#include "ColumnConst.h"
#include "ColumnVector.h"
#include "ColumnMask.h"
#include "Filter.h"

using namespace db;


TEST(FilterTest, MultipleConditions) {
    Filter filter;
    ColumnVector<EdgeTypeID> ids1 {0, 4, 3, 1, 3, 2, 9, 6, 5};
    ColumnVector<EdgeTypeID> ids2 {3, 3, 0, 3, 0, 2, 9, 6, 1};
    ColumnConst<EdgeTypeID> id(3);

    auto mask1 = ColumnMask(ids1.size());
    auto mask2 = ColumnMask(ids1.size());
    auto mask3 = ColumnMask(ids1.size());
    ColumnVector<size_t> indices;
    filter.setIndices(&indices);

    // ids1 == ids2 OR ids1 == 3
    filter.addExpression({db::OP_EQUAL, &mask1, &ids1, &ids2});
    filter.addExpression({db::OP_EQUAL, &mask2, &ids1, &id});
    filter.addExpression({db::OP_OR, &mask3, &mask1, &mask2});
    filter.exec();

    std::vector<bool> comp1 {0, 0, 0, 0, 0, 1, 1, 1, 0};
    std::vector<bool> comp2 {0, 0, 1, 0, 1, 0, 0, 0, 0};
    std::vector<bool> comp3 {0, 0, 1, 0, 1, 1, 1, 1, 0};

    for (const auto& [mask, comp]: ranges::views::zip(mask1, comp1)) {
        ASSERT_EQ(mask._value, comp);
    }

    for (const auto& [mask, comp]: ranges::views::zip(mask2, comp2)) {
        ASSERT_EQ(mask._value, comp);
    }

    for (const auto& [mask, comp]: ranges::views::zip(mask3, comp3)) {
        ASSERT_EQ(mask._value, comp);
    }
}

TEST(FilterTest, EQUAL) {
    Filter filter1;
    Filter filter2;

    ColumnVector<EdgeTypeID> ids1 {0, 4, 3, 1, 3, 2, 9, 6, 5};
    ColumnVector<EdgeTypeID> ids2 {3, 3, 0, 3, 0, 2, 9, 6, 1};
    ColumnConst<EdgeTypeID> id(3);
    ColumnVector<size_t> indices1;
    filter1.setIndices(&indices1);
    ColumnVector<size_t> indices2;
    filter2.setIndices(&indices2);

    auto mask1 = ColumnMask(ids1.size());
    auto mask2 = ColumnMask(ids1.size());

    filter1.addExpression({db::OP_EQUAL, &mask1, &ids1, &ids2});
    filter2.addExpression({db::OP_EQUAL, &mask2, &ids2, &id});
    filter1.exec();
    filter2.exec();

    std::vector<bool> comp1 {0, 0, 0, 0, 0, 1, 1, 1, 0};
    std::vector<bool> comp2 {1, 1, 0, 1, 0, 0, 0, 0, 0};

    for (const auto& [mask, comp]: ranges::views::zip(mask1, comp1)) {
        ASSERT_EQ(mask._value, comp);
    }

    for (const auto& [mask, comp]: ranges::views::zip(mask2, comp2)) {
        ASSERT_EQ(mask._value, comp);
    }
}

TEST(FilterTest, AND) {
    Filter filter;

    ColumnMask mask1 {0, 1, 1, 0, 1, 1, 0, 0, 0};
    ColumnMask mask2 {0, 0, 1, 0, 1, 0, 1, 1, 1};

    auto mask = ColumnMask(mask1.size());
    ColumnVector<size_t> indices;
    filter.setIndices(&indices);

    filter.addExpression({db::OP_AND, &mask, &mask1, &mask2});
    filter.exec();

    std::vector<bool> comp {0, 0, 1, 0, 1, 0, 0, 0, 0};

    for (const auto& [mask, comp]: ranges::views::zip(mask, comp)) {
        ASSERT_EQ(mask._value, comp);
    }
}

TEST(FilterTest, OR) {
    Filter filter;

    ColumnMask mask1 {0, 1, 1, 0, 1, 1, 0, 0, 0};
    ColumnMask mask2 {0, 0, 1, 0, 1, 0, 1, 1, 1};
    ColumnVector<size_t> indices;
    filter.setIndices(&indices);

    auto mask = ColumnMask(mask1.size());

    filter.addExpression({db::OP_OR, &mask, &mask1, &mask2});
    filter.exec();

    std::vector<bool> comp {0, 1, 1, 0, 1, 1, 1, 1, 1};

    for (const auto& [mask, comp]: ranges::views::zip(mask, comp)) {
        ASSERT_EQ(mask._value, comp);
    }
}
