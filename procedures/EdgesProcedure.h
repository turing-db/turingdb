#pragma once

namespace db {

class ProcedureData;
class ProcedureState;
class ProcedureNamespace;

struct EdgesProcedure {
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
