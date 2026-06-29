#pragma once

namespace db {

class ProcedureData;
class ProcedureState;
class ProcedureNamespace;

// Given a labelset return every possible label that forms a valid label sub-set
// with the count of nodes that contain new label sub-set.
//
// For example if we have nodes with the label sets
// - ABC - node count 2
// - ABDE -3
// - ABWZU - 5
// - ABWXY - 10
// - AXCD -100
//
// and we provide [A,B] as the input labelsets - we will get output
//
// C - 2
// D - 3
// E - 3
// W - 15
// Z - 5
// U - 5
// X - 10
// Y - 10
//
struct HierarchicalLabelCountsProcedure {
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
