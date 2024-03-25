#include "mandatory/MandatoryBoolStorage.h"
#include "mandatory/MandatoryGenericStorage.h"
#include "mandatory/MandatoryStringStorage.h"

#include <gtest/gtest.h>
#include <iostream>

using namespace db;

struct TestNode {
    EntityID _id = 0;
    LabelSet _labelset;
    std::string _stringProp;
    bool _boolProp;
    int64_t _intProp;
};

template <typename T>
std::unique_ptr<MandatoryGenericStorage<T>> buildStorage(const std::vector<TestNode>& nodes) {
    typename MandatoryGenericStorage<T>::Builder builder;
    std::unordered_map<LabelSet, size_t> nodeCounts;
    size_t currentPosition = 0;
    std::unordered_map<LabelSet, size_t> currentPositions;

    builder.startBuilding(nodes.size());

    for (const auto& node : nodes) {
        nodeCounts[node._labelset]++;
    }

    for (const auto& [labelset, count] : nodeCounts) {
        builder.addNodeLabelSet(labelset, count, currentPosition);
        currentPositions[labelset] = currentPosition;
        currentPosition += count;
    }

    for (const auto& node : nodes) {
        const size_t pos = currentPositions.at(node._labelset);
        if constexpr (std::is_same_v<T, StringPropertyType>) {
            builder.setProp(node._stringProp, pos);
        } else if constexpr (std::is_same_v<T, BoolPropertyType>) {
            builder.setProp(node._boolProp, pos);
        } else if constexpr (std::is_same_v<T, Int64PropertyType>) {
            builder.setProp(node._intProp, pos);
        }
        currentPositions.at(node._labelset)++;
    }

    PropertyStorage* ptr = builder.build().release();
    return std::unique_ptr<MandatoryGenericStorage<T>> {
        static_cast<MandatoryGenericStorage<T>*>(ptr)};
}

TEST(MandatoryPropertyStorageTest, Create) {
    std::vector<TestNode> nodes(1000);
    std::unordered_map<LabelSet, std::vector<std::string*>> stringDefs;
    std::unordered_map<LabelSet, std::vector<bool>> boolDefs;
    std::unordered_map<LabelSet, std::vector<int64_t>> integerDefs;

    for (size_t i = 0; i < nodes.size(); i++) {
        nodes[i] = {
            ._id = EntityID(i),
            ._labelset = {i % 3},
        };

        stringDefs[nodes[i]._labelset].push_back(&nodes[i]._stringProp);
        boolDefs[nodes[i]._labelset].push_back(nodes[i]._boolProp);
        integerDefs[nodes[i]._labelset].push_back(nodes[i]._intProp);
    }

    auto strings = buildStorage<StringPropertyType>(nodes);

    for (const auto& [labelset, comparison] : stringDefs) {
        const auto s = strings->getSpanFromLabelSet(labelset);

        std::string output1;
        for (const auto& prop : s) {
            output1 += prop + " ";
        }

        std::string output2;
        for (const auto& prop : comparison) {
            output2 += *prop + " ";
        }

        ASSERT_STREQ(output1.c_str(), output2.c_str());
    }

    auto bools = buildStorage<BoolPropertyType>(nodes);
    for (const auto& [labelset, comparison] : boolDefs) {
        const auto b = bools->getSpanFromLabelSet(labelset);

        std::string output1;
        for (const auto& prop : b) {
            output1 += std::to_string(prop);
        }

        std::string output2;
        for (const auto& prop : comparison) {
            output2 += std::to_string(prop);
        }

        ASSERT_STREQ(output1.c_str(), output2.c_str());
    }

    auto integers = buildStorage<Int64PropertyType>(nodes);
    for (const auto& [labelset, comparison] : integerDefs) {
        const auto b = integers->getSpanFromLabelSet(labelset);

        std::string output1;
        for (const auto& prop : b) {
            output1 += std::to_string(prop);
        }

        std::string output2;
        for (const auto& prop : comparison) {
            output2 += std::to_string(prop);
        }

        ASSERT_STREQ(output1.c_str(), output2.c_str());
    }
}
