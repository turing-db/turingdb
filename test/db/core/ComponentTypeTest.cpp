// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "DB.h"
#include "Writeback.h"
#include "ComponentType.h"
#include "Property.h"

using namespace db;

TEST(ComponentTypeTest, createEmpty) {
    DB* db = DB::create();
    Writeback wb(db);

    const std::string compName = "CellularLocation";
    ComponentType* compType = wb.createComponentType(db->getString(compName));
    ASSERT_TRUE(compType);
    ASSERT_EQ(compType->getName(), db->getString(compName));

    delete db;
}

TEST(ComponentTypeTest, createEmptyNameCollision) {
    DB* db = DB::create();
    Writeback wb(db);

    const std::string compName = "CellularLocation";
    ComponentType* type1 = wb.createComponentType(db->getString(compName));
    ASSERT_TRUE(type1);
    ComponentType* type2 = wb.createComponentType(db->getString(compName));
    ASSERT_FALSE(type2);

    delete db;
}

TEST(ComponentTypeTest, addPropertyNameCollision) {
    DB* db = DB::create();
    Writeback wb(db);

    const std::string compName = "Metabolite";
    ComponentType* compType = wb.createComponentType(db->getString(compName));
    ASSERT_TRUE(compType);

    Property* mass = wb.addProperty(compType,
                                    db->getString("mass"),
                                    db->getDecimalType());
    ASSERT_TRUE(mass);
    ASSERT_EQ(mass->getName(), db->getString("mass"));

    Property* mass2 = wb.addProperty(compType,
                                     db->getString("mass"),
                                     db->getDecimalType());
    ASSERT_FALSE(mass2);

    delete db;
}
