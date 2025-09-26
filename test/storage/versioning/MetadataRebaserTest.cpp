#include "gtest/gtest.h"

#include "metadata/LabelMap.h"
#include "metadata/LabelSetMap.h"
#include "versioning/MetadataRebaser.h"
#include "metadata/GraphMetadata.h"
#include "versioning/CommitWriteBuffer.h"
#include "writers/MetadataBuilder.h"

using namespace db;

class MetadataRebaserTest : public ::testing::Test {
protected:
    GraphMetadata _emptyData;
};

TEST_F(MetadataRebaserTest, labels) {
    GraphMetadata dataA;
    GraphMetadata dataB;
    CommitWriteBuffer cwb;

    auto builderA = MetadataBuilder::create(_emptyData, &dataA);
    auto builderB = MetadataBuilder::create(_emptyData, &dataB);

    builderA->getOrCreateLabel("Protein");
    builderA->getOrCreateLabel("Gene");
    builderA->getOrCreateLabel("Disease");

    builderB->getOrCreateLabel("Gene");
    builderB->getOrCreateLabel("Tissue");
    builderB->getOrCreateLabel("Protein");

    MetadataRebaser rebaser;
    rebaser.rebase(dataA, *builderB, cwb);

    ASSERT_EQ(dataB.labels().get("Protein")->getValue(), 0);
    ASSERT_EQ(dataB.labels().get("Gene")->getValue(), 1);
    ASSERT_EQ(dataB.labels().get("Disease")->getValue(), 2);
    ASSERT_EQ(dataB.labels().get("Tissue")->getValue(), 3);

    ASSERT_EQ(dataB.labels().getCount(), 4);

    // Mapping
    ASSERT_EQ(rebaser.getLabelMapping(0), 1);
    ASSERT_EQ(rebaser.getLabelMapping(1), 3);
    ASSERT_EQ(rebaser.getLabelMapping(2), 0);
}

TEST_F(MetadataRebaserTest, edgeTypes) {
    GraphMetadata dataA;
    GraphMetadata dataB;
    CommitWriteBuffer cwb;

    auto builderA = MetadataBuilder::create(_emptyData, &dataA);
    auto builderB = MetadataBuilder::create(_emptyData, &dataB);

    builderA->getOrCreateEdgeType("Interacts");
    builderA->getOrCreateEdgeType("IsPartOf");
    builderA->getOrCreateEdgeType("IsA");

    builderB->getOrCreateEdgeType("IsPartOf");
    builderB->getOrCreateEdgeType("IsA");
    builderB->getOrCreateEdgeType("Interacts");
    builderB->getOrCreateEdgeType("Knows");
    builderB->getOrCreateEdgeType("AssociatedWith");

    MetadataRebaser rebaser;
    rebaser.rebase(dataA, *builderB, cwb);

    ASSERT_EQ(dataB.edgeTypes().get("Interacts")->getValue(), 0);
    ASSERT_EQ(dataB.edgeTypes().get("IsPartOf")->getValue(), 1);
    ASSERT_EQ(dataB.edgeTypes().get("IsA")->getValue(), 2);
    ASSERT_EQ(dataB.edgeTypes().get("Knows")->getValue(), 3);
    ASSERT_EQ(dataB.edgeTypes().get("AssociatedWith")->getValue(), 4);

    ASSERT_EQ(dataB.edgeTypes().getCount(), 5);

    // Mapping
    ASSERT_EQ(rebaser.getEdgeTypeMapping(0), 1);
    ASSERT_EQ(rebaser.getEdgeTypeMapping(1), 2);
    ASSERT_EQ(rebaser.getEdgeTypeMapping(2), 0);
    ASSERT_EQ(rebaser.getEdgeTypeMapping(3), 3);
    ASSERT_EQ(rebaser.getEdgeTypeMapping(4), 4);
}

TEST_F(MetadataRebaserTest, propertyTypes) {
    GraphMetadata dataA;
    GraphMetadata dataB;
    CommitWriteBuffer cwb;

    auto builderA = MetadataBuilder::create(_emptyData, &dataA);
    auto builderB = MetadataBuilder::create(_emptyData, &dataB);

    builderA->getOrCreatePropertyType("name", ValueType::String);
    builderA->getOrCreatePropertyType("age", ValueType::UInt64);
    builderA->getOrCreatePropertyType("weight", ValueType::Double);

    builderB->getOrCreatePropertyType("name", ValueType::String);
    builderB->getOrCreatePropertyType("age", ValueType::UInt64);
    builderB->getOrCreatePropertyType("weight", ValueType::UInt64); // Value type conflict
    builderB->getOrCreatePropertyType("height", ValueType::Double);

    MetadataRebaser rebaser;
    rebaser.rebase(dataA, *builderB, cwb);

    ASSERT_EQ(dataB.propTypes().get("name")->_id, 0);
    ASSERT_EQ(dataB.propTypes().get("age")->_id, 1);
    ASSERT_EQ(dataB.propTypes().get("weight")->_id, 2);
    ASSERT_EQ(dataB.propTypes().get("weight")->_valueType, ValueType::Double);
    ASSERT_EQ(dataB.propTypes().get("weight (UInt64)")->_id, 3);
    ASSERT_EQ(dataB.propTypes().get("weight (UInt64)")->_valueType, ValueType::UInt64);
    ASSERT_EQ(dataB.propTypes().get("height")->_id, 4);

    // Mapping
    ASSERT_EQ(dataB.propTypes().getCount(), 5);

    ASSERT_EQ(rebaser.getPropertyTypeMapping(0)._id, 0);
    ASSERT_EQ(rebaser.getPropertyTypeMapping(1)._id, 1);
    ASSERT_EQ(rebaser.getPropertyTypeMapping(2)._id, 3);
    ASSERT_EQ(rebaser.getPropertyTypeMapping(3)._id, 4);
}

TEST_F(MetadataRebaserTest, labelsets) {
    GraphMetadata dataA;
    GraphMetadata dataB;
    CommitWriteBuffer cwb;

    auto builderA = MetadataBuilder::create(_emptyData, &dataA);
    auto builderB = MetadataBuilder::create(_emptyData, &dataB);

    builderA->getOrCreateLabel("Protein");
    builderA->getOrCreateLabel("Gene");
    builderA->getOrCreateLabel("Disease");
    builderA->getOrCreateLabel("Compound");

    builderA->getOrCreateLabelSet(LabelSet::fromList({0, 1}));
    builderA->getOrCreateLabelSet(LabelSet::fromList({0, 1, 2}));
    builderA->getOrCreateLabelSet(LabelSet::fromList({0, 2}));

    ASSERT_EQ(dataA.labels().getCount(), 4);
    ASSERT_EQ(dataA.labelsets().getCount(), 3);
    
    builderB->getOrCreateLabel("Gene");
    builderB->getOrCreateLabel("Tissue");
    builderB->getOrCreateLabel("Protein");

    builderB->getOrCreateLabelSet(LabelSet::fromList({0, 2}));
    builderB->getOrCreateLabelSet(LabelSet::fromList({1}));
    ASSERT_EQ(dataB.labels().getCount(), 3);
    ASSERT_EQ(dataB.labelsets().getCount(), 2);

    MetadataRebaser rebaser;
    rebaser.rebase(dataA, *builderB, cwb);
    ASSERT_EQ(dataB.labels().get("Protein")->getValue(), 0);
    ASSERT_EQ(dataB.labels().get("Gene")->getValue(), 1);
    ASSERT_EQ(dataB.labels().get("Disease")->getValue(), 2);
    ASSERT_EQ(dataB.labels().get("Compound")->getValue(), 3);
    ASSERT_EQ(dataB.labels().get("Tissue")->getValue(), 4);

    ASSERT_EQ(dataB.labels().getCount(), 5);
    ASSERT_EQ(dataB.labelsets().getCount(), 4);

    ASSERT_EQ(dataB.labelsets().getValue(0)->size(), 2);
    ASSERT_TRUE(dataB.labelsets().getValue(0)->hasLabel(0));
    ASSERT_TRUE(dataB.labelsets().getValue(0)->hasLabel(1));

    ASSERT_EQ(dataB.labelsets().getValue(1)->size(), 3);
    ASSERT_TRUE(dataB.labelsets().getValue(1)->hasLabel(0));
    ASSERT_TRUE(dataB.labelsets().getValue(1)->hasLabel(1));
    ASSERT_TRUE(dataB.labelsets().getValue(1)->hasLabel(2));

    ASSERT_EQ(dataB.labelsets().getValue(2)->size(), 2);
    ASSERT_TRUE(dataB.labelsets().getValue(2)->hasLabel(0));
    ASSERT_TRUE(dataB.labelsets().getValue(2)->hasLabel(2));

    ASSERT_EQ(dataB.labelsets().getValue(3)->size(), 1);
    ASSERT_TRUE(dataB.labelsets().getValue(3)->hasLabel(4));

    // Mapping
    ASSERT_EQ(rebaser.getLabelSetMapping(0).getID(), 0);
    ASSERT_EQ(rebaser.getLabelSetMapping(1).getID(), 3);

}
