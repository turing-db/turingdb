// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "DB.h"
#include "Writeback.h"
#include "NodeType.h"
#include "EdgeType.h"

using namespace db;

TEST(NodeTypeTest, createEmpty) {
    DB* db = DB::create();
    Writeback wb(db);

    NodeType* nodeType = wb.createNodeType(db->getString("Metabolite"));
    ASSERT_TRUE(nodeType);
    ASSERT_EQ(nodeType->getName(), db->getString("Metabolite"));

    delete db;
}

TEST(NodeTypeTest, createEmptyNameCollision) {
    DB* db = DB::create();
    Writeback wb(db);

    NodeType* type1 = wb.createNodeType(db->getString("Metabolite"));
    ASSERT_TRUE(type1);
    NodeType* type2 = wb.createNodeType(db->getString("Metabolite"));
    ASSERT_FALSE(type2);

    delete db;
}

TEST(NodeTypeTest, create2) {
    DB* db = DB::create();
    Writeback wb(db);

    // CellularLocation
    NodeType* cellularLoc = wb.createNodeType(db->getString("CellularLocation"));
    ASSERT_TRUE(cellularLoc);
    
    PropertyType* nameProp = wb.addPropertyType(cellularLoc,
                                                db->getString("Name"),
                                                ValueType::stringType());
    ASSERT_TRUE(nameProp);

    PropertyType* goIDProp = wb.addPropertyType(cellularLoc,
                                                db->getString("GO_ID"),
                                                ValueType::stringType());
    ASSERT_TRUE(goIDProp);

    PropertyType* typeProp = wb.addPropertyType(cellularLoc,
                                                db->getString("LocationType"),
                                                ValueType::stringType());
    ASSERT_TRUE(typeProp);

    // ClinicalInfo
    NodeType* clinicalInfo = wb.createNodeType(db->getString("ClinicalInfo"));
    ASSERT_TRUE(clinicalInfo);

    PropertyType* patientsHigh = wb.addPropertyType(clinicalInfo,
                                                    db->getString("Patients_High"),
                                                    ValueType::decimalType());
    EXPECT_TRUE(patientsHigh);
    PropertyType* patientsLow = wb.addPropertyType(clinicalInfo,
                                                   db->getString("Patients_Low"),
                                                   ValueType::decimalType());
    EXPECT_TRUE(patientsLow);
    PropertyType* patientsMedium = wb.addPropertyType(clinicalInfo,
                                                      db->getString("Patients_Medium"),
                                                      ValueType::decimalType());
    EXPECT_TRUE(patientsMedium);
    PropertyType* patientsNot = wb.addPropertyType(clinicalInfo,
                                                   db->getString("Patients_Not"),
                                                   ValueType::decimalType());
    EXPECT_TRUE(patientsNot);
    PropertyType* prognosticNotOk = wb.addPropertyType(clinicalInfo,
                                                       db->getString("Prognostic_NOT_OK"),
                                                       ValueType::decimalType());
    EXPECT_TRUE(prognosticNotOk);
    PropertyType* prognosticOk = wb.addPropertyType(clinicalInfo,
                                                    db->getString("Prognostic_OK"),
                                                    ValueType::decimalType());
    EXPECT_TRUE(prognosticOk);
    PropertyType* unprognosticNotOkHPA = wb.addPropertyType(clinicalInfo,
                                                            db->getString("UnPrognostic_NOT_OK_HPA"),
                                                            ValueType::decimalType());
    EXPECT_TRUE(unprognosticNotOkHPA);
    PropertyType* unprognosticOkHPA = wb.addPropertyType(clinicalInfo,
                                                         db->getString("UnPrognostic_OK_HPA"),
                                                         ValueType::decimalType());
    EXPECT_TRUE(unprognosticOkHPA);

    // Protein
    NodeType* protein = wb.createNodeType(db->getString("Protein"));
    EXPECT_TRUE(protein);

    // EdgeTypes
    EdgeType* locatedInsideCell = wb.createEdgeType(db->getString("LocatedInsideCell"), protein, cellularLoc);
    EXPECT_TRUE(locatedInsideCell);

    delete db;
}
