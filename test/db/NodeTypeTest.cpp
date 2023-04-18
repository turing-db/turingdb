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
    
    Property* nameProp = wb.addProperty(cellularLoc->getBaseComponent(),
                                        db->getString("Name"),
                                        db->getStringType());
    ASSERT_TRUE(nameProp);

    Property* goIDProp = wb.addProperty(cellularLoc->getBaseComponent(),
                                        db->getString("GO_ID"),
                                        db->getStringType());
    ASSERT_TRUE(goIDProp);

    Property* typeProp = wb.addProperty(cellularLoc->getBaseComponent(),
                                        db->getString("Type"),
                                        db->getStringType());
    ASSERT_TRUE(typeProp);


    // ClinicalInfo
    NodeType* clinicalInfo = wb.createNodeType(db->getString("ClinicalInfo"));
    ASSERT_TRUE(clinicalInfo);

    Property* patientsHigh = wb.addProperty(clinicalInfo->getBaseComponent(),
                                            db->getString("Patients_High"),
                                            db->getDecimalType());
    EXPECT_TRUE(patientsHigh);
    Property* patientsLow = wb.addProperty(clinicalInfo->getBaseComponent(),
                                           db->getString("Patients_Low"),
                                           db->getDecimalType());
    EXPECT_TRUE(patientsLow);
    Property* patientsMedium = wb.addProperty(clinicalInfo->getBaseComponent(),
                                              db->getString("Patients_Medium"),
                                              db->getDecimalType());
    EXPECT_TRUE(patientsMedium);
    Property* patientsNot = wb.addProperty(clinicalInfo->getBaseComponent(),
                                           db->getString("Patients_Not"),
                                           db->getDecimalType());
    EXPECT_TRUE(patientsNot);
    Property* prognosticNotOk = wb.addProperty(clinicalInfo->getBaseComponent(),
                                               db->getString("Prognostic_NOT_OK"),
                                               db->getDecimalType());
    EXPECT_TRUE(prognosticNotOk);
    Property* prognosticOk = wb.addProperty(clinicalInfo->getBaseComponent(),
                                            db->getString("Prognostic_OK"),
                                            db->getDecimalType());
    EXPECT_TRUE(prognosticOk);
    Property* unprognosticNotOkHPA = wb.addProperty(clinicalInfo->getBaseComponent(),
                                                    db->getString("UnPrognostic_NOT_OK_HPA"),
                                                    db->getDecimalType());
    EXPECT_TRUE(unprognosticNotOkHPA);
    Property* unprognosticOkHPA = wb.addProperty(clinicalInfo->getBaseComponent(),
                                                 db->getString("UnPrognostic_OK_HPA"),
                                                 db->getDecimalType());
    EXPECT_TRUE(unprognosticOkHPA);

    // Protein
    NodeType* protein = wb.createNodeType(db->getString("Protein"));
    EXPECT_TRUE(protein);

    // EdgeTypes
    EdgeType* locatedInsideCell = wb.createEdgeType(db->getString("LocatedInsideCell"),
                                                    protein->getBaseComponent(),
                                                    cellularLoc->getBaseComponent());
    EXPECT_TRUE(locatedInsideCell);

    delete db;
}
