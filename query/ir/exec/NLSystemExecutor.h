#pragma once

namespace db {

class NLExecutionContext;
class NLFunctionData;

// The runtime of the system-command family: one handler per nl system op, each
// running once at function scope and filling its command's whole result table in
// that one step.
//
// They are the only handlers that reach past the graph into the server - loading
// a graph, submitting a change, talking to S3 - which they do through the
// NLSystemContext the execution context carries. That is why they sit apart from
// NLExecutor, whose handlers only ever see chunks and the graph view.
class NLSystemExecutor {
public:
    static void runLoadGraph(NLExecutionContext* context, NLFunctionData* data);
    static void runCreateGraph(NLExecutionContext* context, NLFunctionData* data);
    static void runImportGraph(NLExecutionContext* context, NLFunctionData* data);
    static void runListGraphs(NLExecutionContext* context, NLFunctionData* data);
    static void runListAvailableGraphs(NLExecutionContext* context, NLFunctionData* data);

    static void runChangeCommand(NLExecutionContext* context, NLFunctionData* data);
    static void runCommitChange(NLExecutionContext* context, NLFunctionData* data);
    static void runLoadCommit(NLExecutionContext* context, NLFunctionData* data);
    static void runMergeDataParts(NLExecutionContext* context, NLFunctionData* data);

    static void runS3Connect(NLExecutionContext* context, NLFunctionData* data);
    static void runS3Transfer(NLExecutionContext* context, NLFunctionData* data);

    static void runShowProcedures(NLExecutionContext* context, NLFunctionData* data);
    static void runInstallExtension(NLExecutionContext* context, NLFunctionData* data);
    static void runShowExtensions(NLExecutionContext* context, NLFunctionData* data);

    static void runCreateVectorIndex(NLExecutionContext* context, NLFunctionData* data);
    static void runDeleteVectorIndex(NLExecutionContext* context, NLFunctionData* data);
    static void runShowVectorIndexes(NLExecutionContext* context, NLFunctionData* data);
    static void runLoadVector(NLExecutionContext* context, NLFunctionData* data);
    static void runLoadEmbedding(NLExecutionContext* context, NLFunctionData* data);

    static void runCreatePropertyIndex(NLExecutionContext* context, NLFunctionData* data);
    static void runDropIndex(NLExecutionContext* context, NLFunctionData* data);

    static void runExplain(NLExecutionContext* context, NLFunctionData* data);
};

}
