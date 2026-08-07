#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBuffer.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/EntityPropertyView.h"
#include "views/GraphView.h"
#include "views/NodeView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "CypherAST.h"
#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "SinglePartQuery.h"
#include "expr/FunctionInvocationExpr.h"
#include "stmt/CallStmt.h"
#include "stmt/StmtContainer.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "DBDialect.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "IRException.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOps.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// How many times the per-chunk test procedure ran, so a test can assert the execute
// callback is called once per chunk of its argument column rather than once per call.
// Reset before each run.
size_t doubleExecuteCalls = 0;

struct DoubleData : public IndexedProcedureData {};

// test.doubleNodeID(nodeIDs) YIELD doubled: one row per input node, holding the node
// ID doubled. A streaming procedure - it clears and refills its result column on every
// call, and marks itself finished once it has produced a chunk's rows - so it
// exercises the per-chunk execute form (and that the finished flag does not wedge the
// next chunk).
void doubleExecuteImpl(ProcedureState* procedureState) {
    DoubleData& data = procedureState->data<DoubleData>();

    const auto* nodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(0));
    auto* doubled = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));

    doubleExecuteCalls++;

    // Chunking semantics: the result column holds this chunk's rows alone.
    doubled->clear();
    for (const NodeID nodeID : nodeIDs->getRaw()) {
        doubled->push_back(2 * static_cast<types::Int64::Primitive>(nodeID.getValue()));
    }

    procedureState->finish();
}

void doubleExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        doubleExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* doubleAlloc() {
    return new DoubleData();
}

void doubleDealloc(ProcedureData* data) {
    delete data;
}

struct ExpandData : public IndexedProcedureData {};

// test.expandNodeID(nodeIDs) YIELD copy: emits `id % 3` rows for each input node - so
// it drops some input rows, passes others through and expands others - reporting the
// input row behind every row it emits. That report is what lets the caller replicate a
// column carried past the call, so this exercises the carry set: node 0 emits nothing,
// node 1 emits one row, node 2 emits two, and so on.
void expandExecuteImpl(ProcedureState* procedureState) {
    ExpandData& data = procedureState->data<ExpandData>();

    const auto* nodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(0));
    auto* copies = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));
    ColumnIndices* indices = data.indices();

    copies->clear();

    const auto& nodeIDsRaw = nodeIDs->getRaw();
    for (size_t inputRow = 0; inputRow < nodeIDsRaw.size(); inputRow++) {
        const auto nodeID = static_cast<types::Int64::Primitive>(nodeIDsRaw[inputRow].getValue());

        for (types::Int64::Primitive copy = 0; copy < nodeID % 3; copy++) {
            copies->push_back(100 * nodeID + copy);

            // Only a call carrying columns past this one is given the map to report
            // into, so a procedure checks it as it checks an unyielded return column.
            if (indices) {
                indices->push_back(inputRow);
            }
        }
    }

    procedureState->finish();
}

void expandExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        expandExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* expandAlloc() {
    return new ExpandData();
}

void expandDealloc(ProcedureData* data) {
    delete data;
}

struct ScoreData : public IndexedProcedureData {};

// test.nodeScore(nodeIDs) YIELD score: reads each input node's `score` property out of the
// graph and emits it. Unlike the other test procedures - which compute from the node ID
// alone - this one goes through the graph view its context carries, so it exercises a
// procedure that reads the store rather than just its argument column. A node carrying no
// `score` emits nothing, so the property lookup also drives the filtering carry set.
void scoreExecuteImpl(ProcedureState* procedureState) {
    ScoreData& data = procedureState->data<ScoreData>();
    const ProcedureContext* ctxt = procedureState->getContext();

    const auto* nodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(0));
    auto* scores = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));
    ColumnIndices* indices = data.indices();

    scores->clear();

    const GraphReader reader(*ctxt->getGraphView());
    const PropertyTypeMap& propTypes = reader.getMetadata().propTypes();

    // Resolved by name, as a procedure declaring a property argument would have to: the
    // schema is what turns "score" into the ID the node's properties are keyed by.
    const std::optional<PropertyType> scoreType = propTypes.get("score");
    if (!scoreType) {
        procedureState->finish();
        return;
    }

    const auto& nodeIDsRaw = nodeIDs->getRaw();
    for (size_t inputRow = 0; inputRow < nodeIDsRaw.size(); inputRow++) {
        const NodeView node = reader.getNodeView(nodeIDsRaw[inputRow]);
        const types::Int64::Primitive* score = node.properties().tryGetProperty<types::Int64>(scoreType->_id);
        if (!score) {
            continue;
        }

        scores->push_back(*score);

        if (indices) {
            indices->push_back(inputRow);
        }
    }

    procedureState->finish();
}

void scoreExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        scoreExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* scoreAlloc() {
    return new ScoreData();
}

void scoreDealloc(ProcedureData* data) {
    delete data;
}

struct ListData : public IndexedProcedureData {};

// test.nodeIDList(nodeIDs) YIELD values: emits one row per input node whose single column
// is a LIST - `id % 3` elements, `100 * id + k` - so the lists vary in length and nodes 0
// and 3 get an empty one. The list-valued counterpart of test.expandNodeID: the same
// fan-out expressed as one row carrying many values rather than many rows carrying one, so
// this is the row shape a procedure returning a LIST produces. The elements are built into
// the request-scoped ListBuffer the context carries, which is what the ListView rows point
// into, exactly as db.getNodes' `labels` does.
void listExecuteImpl(ProcedureState* procedureState) {
    ListData& data = procedureState->data<ListData>();
    const ProcedureContext* ctxt = procedureState->getContext();

    const auto* nodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(0));
    auto* values = static_cast<ColumnVector<ListView>*>(data.getReturnColumn(0));
    ColumnIndices* indices = data.indices();

    values->clear();

    ListBuffer<4096>* listBuffer = ctxt->getListBuffer();

    std::vector<ListBuffer<4096>::ListItemVariant> items;

    const auto& nodeIDsRaw = nodeIDs->getRaw();
    for (size_t inputRow = 0; inputRow < nodeIDsRaw.size(); inputRow++) {
        const auto nodeID = static_cast<types::Int64::Primitive>(nodeIDsRaw[inputRow].getValue());

        items.clear();
        for (types::Int64::Primitive element = 0; element < nodeID % 3; element++) {
            items.emplace_back(static_cast<types::Int64::Primitive>(100 * nodeID + element));
        }

        // One row per input row, empty list included - a node with nothing to list still
        // gets a row, which is what separates this from the expanding form.
        values->push_back(listBuffer->insert(items));

        if (indices) {
            indices->push_back(inputRow);
        }
    }

    procedureState->finish();
}

void listExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        listExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* listAlloc() {
    return new ListData();
}

void listDealloc(ProcedureData* data) {
    delete data;
}

// The float storage a drive of test.nodeTags hands out spans over. ListBuffer stores the
// bytes of a span, not the floats behind it, so the values have to outlive the insert; they
// live here, refilled per step exactly as the result columns are.
struct TagData : public IndexedProcedureData {
    std::vector<std::vector<float>> _embeddings;
};

// The tags test.nodeTags names, borrowed by the string elements it inserts. Literals, so
// the characters outlive every list that views them - a procedure inserting a
// types::String::Primitive must point at storage that lives at least as long as the row,
// which is why db.getNodes borrows its label names from the label map rather than
// formatting them.
constexpr std::string_view tagNames[] = {"alpha", "beta", "gamma"};

// test.nodeTags(nodeIDs) YIELD tags, coords: two LIST return values per input node - one a
// list of strings (`id % 3` of them, so node 0 gets an empty one), the other a list holding
// a single embedding element, {1.5 * id, 2.5 * id}. An embedding is not a ProcedureType, so
// a list element is the only way a procedure produces one; this covers both of the listable
// element types whose payload is borrowed rather than copied into the buffer.
void tagsExecuteImpl(ProcedureState* procedureState) {
    TagData& data = procedureState->data<TagData>();
    const ProcedureContext* ctxt = procedureState->getContext();

    const auto* nodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(0));
    auto* tags = static_cast<ColumnVector<ListView>*>(data.getReturnColumn(0));
    auto* coords = static_cast<ColumnVector<ListView>*>(data.getReturnColumn(1));
    ColumnIndices* indices = data.indices();

    tags->clear();
    coords->clear();

    ListBuffer<4096>* listBuffer = ctxt->getListBuffer();

    const auto& nodeIDsRaw = nodeIDs->getRaw();

    // Sized once, so filling an entry never relocates one already handed to the buffer.
    data._embeddings.assign(nodeIDsRaw.size(), {});

    std::vector<ListBuffer<4096>::ListItemVariant> items;

    for (size_t inputRow = 0; inputRow < nodeIDsRaw.size(); inputRow++) {
        const auto nodeID = static_cast<types::Int64::Primitive>(nodeIDsRaw[inputRow].getValue());

        items.clear();
        for (types::Int64::Primitive tag = 0; tag < nodeID % 3; tag++) {
            items.emplace_back(types::String::Primitive {tagNames[tag]});
        }
        tags->push_back(listBuffer->insert(items));

        std::vector<float>& embedding = data._embeddings[inputRow];
        embedding = {1.5f * static_cast<float>(nodeID), 2.5f * static_cast<float>(nodeID)};

        items.clear();
        items.emplace_back(types::Embedding::Primitive {embedding});
        coords->push_back(listBuffer->insert(items));

        if (indices) {
            indices->push_back(inputRow);
        }
    }

    procedureState->finish();
}

void tagsExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        tagsExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* tagsAlloc() {
    return new TagData();
}

void tagsDealloc(ProcedureData* data) {
    delete data;
}

// Where a drive of test.fanOutNodeID stopped: the input row it was on and how many of
// that row's copies it had emitted, so the next step resumes there. Reset clears both,
// since a drive starts afresh on each chunk of arguments.
struct FanOutData : public IndexedProcedureData {
    size_t _nextInputRow {0};
    types::Int64::Primitive _nextCopy {0};
};

// test.fanOutNodeID(nodeIDs) YIELD value: emits three rows per input node - 10*id,
// 10*id+1, 10*id+2 - but never more than a chunk's worth per call, resuming where the
// last call stopped and declaring itself finished only once every input row is spent. So
// one chunk of arguments is answered with several chunks of rows, which is what the
// drive loop over nl.procedure_init exists for.
void fanOutExecuteImpl(ProcedureState* procedureState) {
    FanOutData& data = procedureState->data<FanOutData>();

    const auto* nodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(0));
    auto* values = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));
    ColumnIndices* indices = data.indices();

    constexpr types::Int64::Primitive copiesPerRow = 3;
    const size_t chunkSize = procedureState->getContext()->getChunkSize();

    values->clear();

    const auto& nodeIDsRaw = nodeIDs->getRaw();
    while (values->size() < chunkSize && data._nextInputRow < nodeIDsRaw.size()) {
        const auto nodeID = static_cast<types::Int64::Primitive>(nodeIDsRaw[data._nextInputRow].getValue());

        values->push_back(10 * nodeID + data._nextCopy);
        if (indices) {
            indices->push_back(data._nextInputRow);
        }

        data._nextCopy++;
        if (data._nextCopy == copiesPerRow) {
            data._nextCopy = 0;
            data._nextInputRow++;
        }
    }

    // Finished once the last input row's last copy has been emitted - which may be the
    // step that just filled a full chunk, so the flag is set on the step that exhausts
    // the input rather than on an empty one after it.
    if (data._nextInputRow >= nodeIDsRaw.size()) {
        procedureState->finish();
    }
}

void fanOutExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        break;

        case ProcedureState::Step::RESET: {
            FanOutData& data = procedureState->data<FanOutData>();
            data._nextInputRow = 0;
            data._nextCopy = 0;
        }
        break;

        case ProcedureState::Step::EXECUTE:
        fanOutExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* fanOutAlloc() {
    return new FanOutData();
}

void fanOutDealloc(ProcedureData* data) {
    delete data;
}

// Collects the single int64 column a call emits, plus how many chunks it arrived in,
// so a test can assert both the rows and the chunking.
class Int64Sink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnVector<types::Int64::Primitive>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        _calls++;

        const auto& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.push_back(raw[rowIndex]);
        }
    }

    const std::vector<types::Int64::Primitive>& rows() const { return _rows; }
    size_t getCalls() const { return _calls; }

private:
    std::vector<types::Int64::Primitive> _rows;
    size_t _calls {0};
};

// Collects the (label id, label name) rows db.labels emits: a !storage.label_id chunk
// and a borrowed-string chunk, the two column types a procedure yields them as.
class LabelSink : public NLOutputSink {
public:
    using Row = std::pair<uint32_t, std::string>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* ids = dynamic_cast<const ColumnVector<LabelID>*>(chunks[0]);
        const auto* names = dynamic_cast<const ColumnVector<types::String::Primitive>*>(chunks[1]);
        ASSERT_NE(ids, nullptr);
        ASSERT_NE(names, nullptr);
        ASSERT_EQ(ids->size(), names->size());

        _calls++;

        const auto& idRaw = ids->getRaw();
        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(idRaw[rowIndex].getValue(), std::string(nameRaw[rowIndex]));
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    size_t getCalls() const { return _calls; }

private:
    std::vector<Row> _rows;
    size_t _calls {0};
};


// How many times the side-effect procedure ran, so a test can assert a call with no
// result still drives it.
size_t sideEffectCalls = 0;

struct SideEffectData : public ProcedureData {};

// test.sideEffect(): declares no return value at all - it is called for what it does, not
// for rows. Its execute callback emits nothing and finishes immediately.
void sideEffectExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        sideEffectCalls++;
        procedureState->finish();
        break;
    }
}

ProcedureData* sideEffectAlloc() {
    return new SideEffectData();
}

void sideEffectDealloc(ProcedureData* data) {
    delete data;
}

struct TwoNodesData : public ProcedureData {};

// test.twoNodes() YIELD id: emits node IDs 0 and 1, in one chunk, then finishes. A
// source procedure yielding an ID column, so its rows can be crossed with another
// factor's - which is how a call whose rows do not depend on the outer row is paired
// with it.
void twoNodesExecuteImpl(ProcedureState* procedureState) {
    TwoNodesData& data = procedureState->data<TwoNodesData>();

    auto* ids = static_cast<ColumnVector<NodeID>*>(data.getReturnColumn(0));
    ids->clear();
    ids->push_back(NodeID {0});
    ids->push_back(NodeID {1});

    procedureState->finish();
}

void twoNodesExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        twoNodesExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* twoNodesAlloc() {
    return new TwoNodesData();
}

void twoNodesDealloc(ProcedureData* data) {
    delete data;
}

// Collects the (node, label name) rows a call crossed with a scan emits: a node column
// and a borrowed-string column, the pair a MATCH crossed with db.labels produces.
class NodeLabelSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, std::string>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* labels = dynamic_cast<const ColumnVector<types::String::Primitive>*>(chunks[1]);
        ASSERT_NE(nodes, nullptr);
        ASSERT_NE(labels, nullptr);
        ASSERT_EQ(nodes->size(), labels->size());

        const auto& nodeRaw = nodes->getRaw();
        const auto& labelRaw = labels->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(nodeRaw[rowIndex].getValue(), std::string(labelRaw[rowIndex]));
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Counts emissions without materializing them, so a call that produces no column can be
// shown to emit nothing.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _calls++;
        _rows += rowCount;
    }

    size_t getCalls() const { return _calls; }
    size_t getRows() const { return _rows; }

private:
    size_t _calls {0};
    size_t _rows {0};
};

// Collects the single node column a call yielding one NODE return value emits.
class NodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* nodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        ASSERT_NE(nodes, nullptr);

        const auto& raw = nodes->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.push_back(raw[rowIndex].getValue());
        }
    }

    void sortedRows(std::vector<uint64_t>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<uint64_t> _rows;
};

// Collects the (node, node) rows a cross product of a scan and a source call emits.
class NodePairSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* left = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* right = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        ASSERT_NE(left, nullptr);
        ASSERT_NE(right, nullptr);
        ASSERT_EQ(left->size(), right->size());

        const auto& leftRaw = left->getRaw();
        const auto& rightRaw = right->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(leftRaw[rowIndex].getValue(), rightRaw[rowIndex].getValue());
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (node, value) rows of a call that carries one node column past itself,
// so a test can assert the carried node is repeated once per row the procedure emitted
// for it.
class CarriedNodeSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, types::Int64::Primitive>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnVector<types::Int64::Primitive>*>(chunks[1]);
        ASSERT_NE(nodes, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodes->size(), values->size());

        _calls++;

        const auto& nodeRaw = nodes->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(nodeRaw[rowIndex].getValue(), valueRaw[rowIndex]);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    size_t getCalls() const { return _calls; }

private:
    std::vector<Row> _rows;
    size_t _calls {0};
};

// Collects the (node, [values]) rows a call returning a LIST emits: a node ID chunk and a
// list cell chunk (ColumnVector<ListView>), the column type a LIST return value is read as.
class CarriedListSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, std::vector<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(nodes, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(nodes->size(), lists->size());

        const auto& nodeRaw = nodes->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::vector<int64_t> values;
            for (const ListElementView& element : listRaw[rowIndex]) {
                values.push_back(element.getAs<int64_t>());
            }

            _rows.emplace_back(nodeRaw[rowIndex].getValue(), values);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (node, [tags], [coords]) rows of a call returning two LIST columns: one of
// borrowed strings, one holding a single embedding element. Each element's tag is asserted
// before it is read, since getAs does not check the type it is handed.
class CarriedTagsSink : public NLOutputSink {
public:
    using Row = std::tuple<uint64_t, std::vector<std::string>, std::vector<float>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* nodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* tagLists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        const auto* coordLists = dynamic_cast<const ColumnVector<ListView>*>(chunks[2]);
        ASSERT_NE(nodes, nullptr);
        ASSERT_NE(tagLists, nullptr);
        ASSERT_NE(coordLists, nullptr);
        ASSERT_EQ(nodes->size(), tagLists->size());
        ASSERT_EQ(nodes->size(), coordLists->size());

        const auto& nodeRaw = nodes->getRaw();
        const auto& tagRaw = tagLists->getRaw();
        const auto& coordRaw = coordLists->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::vector<std::string> tags;
            for (const ListElementView& element : tagRaw[rowIndex]) {
                ASSERT_EQ(element.getTag(), ListBufferTypeTag::String);
                tags.push_back(std::string(element.getAs<std::string_view>()));
            }

            // The embedding rides as the single element of its own list, so the row's
            // floats are that element's span.
            std::vector<float> coords;
            for (const ListElementView& element : coordRaw[rowIndex]) {
                ASSERT_EQ(element.getTag(), ListBufferTypeTag::Embedding);
                const std::span<const float> embedding = element.getAs<std::span<const float>>();
                coords.assign(embedding.begin(), embedding.end());
            }

            _rows.emplace_back(nodeRaw[rowIndex].getValue(), tags, coords);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (n, m, value) rows of a call that carries both sides of a hop past
// itself - the shape of MATCH (n)-->(m) CALL f(m) YIELD x RETURN n, m, x.
class CarriedHopSink : public NLOutputSink {
public:
    using Row = std::tuple<uint64_t, uint64_t, types::Int64::Primitive>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* sources = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* targets = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        const auto* values = dynamic_cast<const ColumnVector<types::Int64::Primitive>*>(chunks[2]);
        ASSERT_NE(sources, nullptr);
        ASSERT_NE(targets, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(sources->size(), values->size());
        ASSERT_EQ(targets->size(), values->size());

        const auto& sourceRaw = sources->getRaw();
        const auto& targetRaw = targets->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(sourceRaw[rowIndex].getValue(),
                               targetRaw[rowIndex].getValue(),
                               valueRaw[rowIndex]);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (a, b, c) rows of three uncorrelated calls crossed together, so a test can
// assert the full Cartesian product of what each one emitted.
class NodeTripleSink : public NLOutputSink {
public:
    using Row = std::tuple<uint64_t, uint64_t, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* first = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* second = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        const auto* third = dynamic_cast<const ColumnVector<NodeID>*>(chunks[2]);
        ASSERT_NE(first, nullptr);
        ASSERT_NE(second, nullptr);
        ASSERT_NE(third, nullptr);
        ASSERT_EQ(first->size(), second->size());
        ASSERT_EQ(first->size(), third->size());

        const auto& firstRaw = first->getRaw();
        const auto& secondRaw = second->getRaw();
        const auto& thirdRaw = third->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(firstRaw[rowIndex].getValue(),
                               secondRaw[rowIndex].getValue(),
                               thirdRaw[rowIndex].getValue());
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (id, copy, value) rows of a chain of calls: the first call's node yield
// followed by one Int64 yield from each of the two calls after it, so a test can assert
// what survived being carried past several calls rather than one.
class ChainedCallSink : public NLOutputSink {
public:
    using Row = std::tuple<uint64_t, types::Int64::Primitive, types::Int64::Primitive>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* ids = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* copies = dynamic_cast<const ColumnVector<types::Int64::Primitive>*>(chunks[1]);
        const auto* values = dynamic_cast<const ColumnVector<types::Int64::Primitive>*>(chunks[2]);
        ASSERT_NE(ids, nullptr);
        ASSERT_NE(copies, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(ids->size(), copies->size());
        ASSERT_EQ(ids->size(), values->size());

        const auto& idRaw = ids->getRaw();
        const auto& copyRaw = copies->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(idRaw[rowIndex].getValue(), copyRaw[rowIndex], valueRaw[rowIndex]);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the rows of a projection of only constants, which are emitted from a
// ColumnConst broadcast against the driving loop's row count rather than a per-row
// column.
template <typename T>
class ConstSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnConst<T>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.push_back((*values)[rowIndex]);
        }
    }

    const std::vector<T>& rows() const { return _rows; }

private:
    std::vector<T> _rows;
};

// Collects the single label-name column a call that yields only `label` emits, so a
// test can assert the return values it did not yield are left unbound.
class LabelNameSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* names = dynamic_cast<const ColumnVector<types::String::Primitive>*>(chunks[0]);
        ASSERT_NE(names, nullptr);

        const auto& raw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(raw[rowIndex]);
        }
    }

    void sortedRows(std::vector<std::string>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::string> _rows;
};

// CALL db.labels() YIELD id, label: the source form. db.labels takes no argument, so
// it produces rows on its own and the call lowers to a drive loop that runs it until
// it declares itself finished.
constexpr const char* labelsProgram = R"mlir(
func.func @main() {
  %id, %label = db.call_procedure("db.labels", {}, {}) yields ["id", "label"] : () -> (!db.column<none>, !db.column<none>)
  db.output(%id, %label) : !db.column<none>, !db.column<none>
  return
}
)mlir";

// CALL db.labels() YIELD label: the same source call binding only one of the two
// return values, so the `id` column stays unbound and the procedure skips it.
constexpr const char* labelNamesProgram = R"mlir(
func.func @main() {
  %label = db.call_procedure("db.labels", {}, {}) yields ["label"] : () -> !db.column<none>
  db.output(%label) : !db.column<none>
  return
}
)mlir";

// CALL db.labels() YIELD label RETURN 5: a projection of only constants over a drive
// loop. The constant has no per-row column to size the emission, so the output nests
// in the drive loop and takes its row count from a loop variable - here a label-ID
// chunk, not the node-ID chunk a scan would bind, which is why nl.output's cardinality
// driver accepts a chunk of any element type.
constexpr const char* labelsConstantProgram = R"mlir(
func.func @main() {
  %label = db.call_procedure("db.labels", {}, {}) yields ["label"] : () -> !db.column<none>
  %c = db.constant(5 : i64)
  db.output(%c) : !db.column<i64>
  return
}
)mlir";

// MATCH (n) CALL test.doubleNodeID(n) YIELD doubled RETURN doubled: the per-chunk
// form. The procedure's argument is the scan's node column, so the call sits in the
// scan loop and runs once per chunk.
constexpr const char* doubleNodeIDProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %doubled = db.call_procedure("test.doubleNodeID", {%a}, {}) yields ["doubled"] : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%doubled) : !db.column<none>
  return
}
)mlir";

// MATCH (n) CALL test.expandNodeID(n) YIELD copy RETURN n, copy: the carry set on its
// own. The procedure emits a different number of rows than it was given, so `n` cannot
// flow around the call - it rides through the carry set and comes back replicated once
// per row the procedure emitted for it.
constexpr const char* expandCarryProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %copy, %a2 = db.call_procedure("test.expandNodeID", {%a}, {%a}) yields ["copy"] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  db.output(%a2, %copy) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (n)-->(m) CALL test.expandNodeID(m) YIELD copy RETURN n, m, copy: the carry set
// past a hop. The hop already carries `n` alongside `m`, and the call carries both, so
// the projection stays row-aligned with what the procedure emitted for each `m`.
constexpr const char* hopExpandCarryProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %m = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %copy, %n2, %m2 = db.call_procedure("test.expandNodeID", {%m}, {%srcs, %m}) yields ["copy"] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%n2, %m2, %copy) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (n) CALL test.fanOutNodeID(n) YIELD value RETURN n, value: one chunk of
// arguments answered with several chunks of rows. The procedure emits three rows per
// input node but at most a chunk's worth per call, so the drive loop runs it repeatedly
// over the same chunk of `n` - and the carried `n` is rebuilt for each of those chunks.
constexpr const char* fanOutCarryProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %value, %a2 = db.call_procedure("test.fanOutNodeID", {%a}, {%a}) yields ["value"] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  db.output(%a2, %value) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// A carry set on a procedure that does not declare it reports input rows: refused while
// the call is planned, since the carried column could only be rebuilt from that report.
constexpr const char* undeclaredCarryProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %doubled, %a2 = db.call_procedure("test.doubleNodeID", {%a}, {%a}) yields ["doubled"] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  db.output(%a2, %doubled) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// A carry set on a procedure that declares the report but never makes it: the plan-time
// check passes on its word, so this is caught at the first chunk it emits.
constexpr const char* brokenReportCarryProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %doubled, %a2 = db.call_procedure("test.brokenReportNodeID", {%a}, {%a}) yields ["doubled"] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  db.output(%a2, %doubled) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// A carry set on an argument-less procedure: it reads no input row to replicate the
// carried rows against.
constexpr const char* sourceCarryProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %label, %a2 = db.call_procedure("db.labels", {}, {%a}) yields ["label"] : (!db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  db.output(%a2, %label) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (n), CALL test.twoNodes() YIELD id RETURN n, id: a call whose rows do not depend
// on the outer row, paired with it by a cross product. The call is a factor of the
// product, so its loop must open inside the other factor's nest - not at function scope,
// where it could not see the scan's chunk.
constexpr const char* crossedSourceCallProgram = R"mlir(
func.func @main() {
  %n, %id = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %x = db.call_procedure("test.twoNodes", {}, {}) yields ["id"] : () -> !db.column<!storage.node_id>
    db.yield %x : !db.column<!storage.node_id>
  }
  db.output(%n, %id) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A call of a procedure that is not registered: caught when the name is resolved
// against the registry during lowering.
constexpr const char* unknownProcedureProgram = R"mlir(
func.func @main() {
  %id = db.call_procedure("db.doesNotExist", {}, {}) yields ["id"] : () -> !db.column<none>
  db.output(%id) : !db.column<none>
  return
}
)mlir";

// A call yielding a name db.labels does not return.
constexpr const char* unknownYieldProgram = R"mlir(
func.func @main() {
  %x = db.call_procedure("db.labels", {}, {}) yields ["notAReturnValue"] : () -> !db.column<none>
  db.output(%x) : !db.column<none>
  return
}
)mlir";

// A call passing an argument to a procedure that declares none.
constexpr const char* tooManyArgumentsProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.call_procedure("db.labels", {%a}, {}) yields ["id"] : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%id) : !db.column<none>
  return
}
)mlir";

}

class MLIRCallProcedureTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        // The db namespace, so a test can call a real registered procedure, plus a
        // test namespace holding the procedures that exercise the per-chunk form - no
        // registered procedure takes a per-row column argument.
        _procedures.init();

        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* doubleProcedure = new Procedure("doubleNodeID");
        doubleProcedure->setAllocCallback(&doubleAlloc);
        doubleProcedure->setDeallocCallback(&doubleDealloc);
        doubleProcedure->setExecuteCallback(&doubleExecute);
        doubleProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        doubleProcedure->addReturnValue("doubled", ProcedureType::INT64);
        testNamespace->addProcedure(doubleProcedure);

        Procedure* expandProcedure = new Procedure("expandNodeID");
        expandProcedure->setAllocCallback(&expandAlloc);
        expandProcedure->setDeallocCallback(&expandDealloc);
        expandProcedure->setExecuteCallback(&expandExecute);
        expandProcedure->setHasIndices(true);
        expandProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        expandProcedure->addReturnValue("copy", ProcedureType::INT64);
        testNamespace->addProcedure(expandProcedure);

        Procedure* fanOutProcedure = new Procedure("fanOutNodeID");
        fanOutProcedure->setAllocCallback(&fanOutAlloc);
        fanOutProcedure->setDeallocCallback(&fanOutDealloc);
        fanOutProcedure->setExecuteCallback(&fanOutExecute);
        fanOutProcedure->setHasIndices(true);
        fanOutProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        fanOutProcedure->addReturnValue("value", ProcedureType::INT64);
        testNamespace->addProcedure(fanOutProcedure);

        Procedure* scoreProcedure = new Procedure("nodeScore");
        scoreProcedure->setAllocCallback(&scoreAlloc);
        scoreProcedure->setDeallocCallback(&scoreDealloc);
        scoreProcedure->setExecuteCallback(&scoreExecute);
        scoreProcedure->setHasIndices(true);
        scoreProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        scoreProcedure->addReturnValue("score", ProcedureType::INT64);
        testNamespace->addProcedure(scoreProcedure);

        Procedure* listProcedure = new Procedure("nodeIDList");
        listProcedure->setAllocCallback(&listAlloc);
        listProcedure->setDeallocCallback(&listDealloc);
        listProcedure->setExecuteCallback(&listExecute);
        listProcedure->setHasIndices(true);
        listProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        listProcedure->addReturnValue("values", ProcedureType::LIST);
        testNamespace->addProcedure(listProcedure);

        Procedure* tagsProcedure = new Procedure("nodeTags");
        tagsProcedure->setAllocCallback(&tagsAlloc);
        tagsProcedure->setDeallocCallback(&tagsDealloc);
        tagsProcedure->setExecuteCallback(&tagsExecute);
        tagsProcedure->setHasIndices(true);
        tagsProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        tagsProcedure->addReturnValue("tags", ProcedureType::LIST);
        tagsProcedure->addReturnValue("coords", ProcedureType::LIST);
        testNamespace->addProcedure(tagsProcedure);

        // Declares the report but never makes it - the only way to reach the runtime
        // check now that lowering refuses a carry set on a procedure that declares
        // nothing.
        Procedure* brokenProcedure = new Procedure("brokenReportNodeID");
        brokenProcedure->setAllocCallback(&doubleAlloc);
        brokenProcedure->setDeallocCallback(&doubleDealloc);
        brokenProcedure->setExecuteCallback(&doubleExecute);
        brokenProcedure->setHasIndices(true);
        brokenProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        brokenProcedure->addReturnValue("doubled", ProcedureType::INT64);
        testNamespace->addProcedure(brokenProcedure);

        Procedure* sideEffectProcedure = new Procedure("sideEffect");
        sideEffectProcedure->setAllocCallback(&sideEffectAlloc);
        sideEffectProcedure->setDeallocCallback(&sideEffectDealloc);
        sideEffectProcedure->setExecuteCallback(&sideEffectExecute);
        testNamespace->addProcedure(sideEffectProcedure);

        Procedure* twoNodesProcedure = new Procedure("twoNodes");
        twoNodesProcedure->setAllocCallback(&twoNodesAlloc);
        twoNodesProcedure->setDeallocCallback(&twoNodesDealloc);
        twoNodesProcedure->setExecuteCallback(&twoNodesExecute);
        twoNodesProcedure->addReturnValue("id", ProcedureType::NODE);
        testNamespace->addProcedure(twoNodesProcedure);

    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Five labels and five nodes carrying the first of them, so a drive of db.labels
    // and a scan of the nodes both span several chunks at a chunk size below five.
    std::unique_ptr<Graph> buildLabelledGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("Person");
        metadata.getOrCreateLabel("Employee");
        metadata.getOrCreateLabel("Manager");
        metadata.getOrCreateLabel("Contractor");
        metadata.getOrCreateLabel("Intern");

        const LabelSet labelset = LabelSet::fromList({0});
        for (size_t nodeIndex = 0; nodeIndex < 5; nodeIndex++) {
            builder.addNode(labelset);
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Five nodes (0..4) and four edges - 0->1, 0->2, 1->4, 2->3 - so a hop produces the
    // (n, m) pairs (0,1), (0,2), (1,4) and (2,3). Run through test.expandNodeID, whose
    // fan-out is `m % 3`, those pairs expand to one row for m=1, two for m=2, one for
    // m=4 and none for m=3 - so one graph exercises passing a row through, expanding it
    // and dropping it.
    std::unique_ptr<Graph> buildHopGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID node0 = builder.addNode(labelset);
        const NodeID node1 = builder.addNode(labelset);
        const NodeID node2 = builder.addNode(labelset);
        const NodeID node3 = builder.addNode(labelset);
        const NodeID node4 = builder.addNode(labelset);

        builder.addEdge(0, node0, node1);
        builder.addEdge(0, node0, node2);
        builder.addEdge(0, node1, node4);
        builder.addEdge(0, node2, node3);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Five nodes carrying an Int64 `score` property, except nodes 2 and 4, which carry
    // none. test.nodeScore reads that property out of the graph, so the scores are
    // deliberately unrelated to the node IDs - a procedure echoing its argument column
    // would produce different rows - and the two nodes without one exercise the drop.
    std::unique_ptr<Graph> buildScoreGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("Player");
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID node0 = builder.addNode(labelset);
        const NodeID node1 = builder.addNode(labelset);
        builder.addNode(labelset);
        const NodeID node3 = builder.addNode(labelset);
        builder.addNode(labelset);

        builder.addNodeProperty<types::Int64>(node0, scoreID, 10);
        builder.addNodeProperty<types::Int64>(node1, scoreID, 20);
        builder.addNodeProperty<types::Int64>(node3, scoreID, 30);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Parses a db-dialect program, lowers it to nl with DBLowering - resolving the
    // call against the test's registry - and runs the lowered function against the
    // graph view. The procedure context is what a CALL needs at both stages: the
    // registry to resolve the name, and the view, chunk size and list buffer its
    // callbacks read. The chunk size is exposed so a test can force a call to span
    // chunk boundaries.
    void runLoweredProgram(const char* programText,
                           const GraphView& view,
                           NLOutputSink& sink,
                           size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view, &_procedures);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;

        ProcedureContext procedureContext;
        procedureContext.setGraphView(&view);
        procedureContext.setProcedures(&_procedures);
        procedureContext.setChunkSize(chunkSize);
        procedureContext.setListBuffer(&memory.listBuffer());

        NLInterpreter interpreter(*nlModule,
                                  &view,
                                  &sink,
                                  &memory,
                                  chunkSize,
                                  /*writeBuffer=*/nullptr,
                                  /*metadataBuilder=*/nullptr,
                                  &procedureContext);
        interpreter.run();
    }

    // Runs a Cypher query the whole way down: parsed and analyzed against the test's own
    // procedure registry, generated into db dialect by DBProgramGenerator, lowered and
    // executed. This is the path a CALL takes from the query text, so it covers the
    // frontend as well as the engine.
    void runQuery(const char* queryText,
                  Graph* graph,
                  const GraphView& view,
                  NLOutputSink& sink,
                  size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        CypherAST ast(&_procedures, queryText);

        CypherParser parser(&ast);
        parser.parse(queryText);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = dbModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view, &_procedures);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;

        // A procedure reads the request through this context, so it carries everything one
        // may ask for: the graph and the transaction as well as the view, since a
        // version-control procedure like db.history reads the commit being queried.
        Transaction transaction(graph->openTransaction());

        ProcedureContext procedureContext;
        procedureContext.setGraph(graph);
        procedureContext.setGraphView(&view);
        procedureContext.setTransaction(&transaction);
        procedureContext.setProcedures(&_procedures);
        procedureContext.setChunkSize(chunkSize);
        procedureContext.setListBuffer(&memory.listBuffer());

        NLInterpreter interpreter(*nlModule,
                                  &view,
                                  &sink,
                                  &memory,
                                  chunkSize,
                                  /*writeBuffer=*/nullptr,
                                  /*metadataBuilder=*/nullptr,
                                  &procedureContext);
        interpreter.run();
    }

    // Lowers a program the same way but without running it, so a test can assert the
    // errors lowering raises. Takes the whole module by value, as the runner does, so
    // the MLIR context outlives the lowering.
    void lowerProgram(const char* programText, const GraphView& view) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view, &_procedures);
        lowering.lower(dbFunction, *nlModule);
    }

    ProcedureManager _procedures;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(MLIRCallProcedureTest, sourceCallYieldsEveryRow) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The five labels of the graph, each with the ID the schema gave it.
    LabelSink sink;
    runLoweredProgram(labelsProgram, reader.getView(), sink);

    std::vector<LabelSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<LabelSink::Row> expected {{0, "Person"},
                                                {1, "Employee"},
                                                {2, "Manager"},
                                                {3, "Contractor"},
                                                {4, "Intern"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, sourceCallSpansChunks) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Five labels at a chunk size of two: the drive loop must run the procedure again
    // after each chunk, so the rows arrive in three chunks (2, 2, 1) and none is lost.
    LabelSink sink;
    runLoweredProgram(labelsProgram, reader.getView(), sink, /*chunkSize=*/2);

    std::vector<LabelSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows.size(), 5u);
    EXPECT_EQ(sink.getCalls(), 3u);
}

TEST_F(MLIRCallProcedureTest, sourceCallBindsOnlyYieldedReturnValues) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Yielding only `label` leaves the procedure's `id` return value unbound, so it
    // fills the name column alone.
    LabelNameSink sink;
    runLoweredProgram(labelNamesProgram, reader.getView(), sink);

    std::vector<std::string> rows;
    sink.sortedRows(rows);
    const std::vector<std::string> expected {"Contractor", "Employee", "Intern", "Manager", "Person"};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, constantProjectionOverDriveLoopKeepsCardinality) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Projection preserves cardinality, so the five labels give five rows of 5. The
    // drive loop binds no node-ID chunk, so this only lowers because nl.output's
    // cardinality driver takes a chunk of any element type.
    ConstSink<types::Int64::Primitive> sink;
    runLoweredProgram(labelsConstantProgram, reader.getView(), sink, /*chunkSize=*/2);

    const std::vector<types::Int64::Primitive> expected {5, 5, 5, 5, 5};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(MLIRCallProcedureTest, perChunkCallRunsOncePerChunk) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Five nodes at a chunk size of two: the scan produces three chunks, so the
    // procedure is called three times and each call's rows are the chunk's alone -
    // doubling node IDs 0..4 exactly once each, nothing accumulated across chunks.
    doubleExecuteCalls = 0;

    Int64Sink sink;
    runLoweredProgram(doubleNodeIDProgram, reader.getView(), sink, /*chunkSize=*/2);

    const std::vector<types::Int64::Primitive> expected {0, 2, 4, 6, 8};
    EXPECT_EQ(sink.rows(), expected);
    EXPECT_EQ(sink.getCalls(), 3u);
    EXPECT_EQ(doubleExecuteCalls, 3u);
}

TEST_F(MLIRCallProcedureTest, carriedColumnIsReplicatedPerEmittedRow) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // test.expandNodeID emits `id % 3` rows per node, so nodes 0 and 3 emit nothing,
    // nodes 1 and 4 one row each and node 2 two rows. The carried `n` follows: it is
    // dropped where the procedure emitted no row and repeated where it emitted two.
    CarriedNodeSink sink;
    runLoweredProgram(expandCarryProgram, reader.getView(), sink, /*chunkSize=*/2);

    std::vector<CarriedNodeSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<CarriedNodeSink::Row> expected {{1, 100}, {2, 200}, {2, 201}, {4, 400}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, oneArgumentChunkYieldsSeveralRowChunks) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Five nodes arrive as one chunk of arguments (chunk size 5), and the procedure
    // answers it with fifteen rows - three per node - at most five per call. So the
    // drive loop runs it three times over that one chunk, and the carried `n` is rebuilt
    // for each of the three chunks it emits.
    CarriedNodeSink sink;
    runLoweredProgram(fanOutCarryProgram, reader.getView(), sink, /*chunkSize=*/5);

    std::vector<CarriedNodeSink::Row> rows;
    sink.sortedRows(rows);

    std::vector<CarriedNodeSink::Row> expected;
    for (uint64_t nodeID = 0; nodeID < 5; nodeID++) {
        for (types::Int64::Primitive copy = 0; copy < 3; copy++) {
            expected.emplace_back(nodeID, 10 * static_cast<types::Int64::Primitive>(nodeID) + copy);
        }
    }

    EXPECT_EQ(rows, expected);
    EXPECT_EQ(sink.getCalls(), 3u);
}

TEST_F(MLIRCallProcedureTest, carriedColumnsSurviveAHopIntoACall) {
    auto graph = buildHopGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n)-->(m) CALL test.expandNodeID(m) YIELD copy RETURN n, m, copy over the
    // hop pairs (0,1), (0,2), (1,4) and (2,3): m=1 emits one row, m=2 two, m=4 one and
    // m=3 none. Both sides of the hop ride the carry set, so (0,2) appears twice - once
    // per row the procedure emitted for m=2 - and (2,3) not at all.
    CarriedHopSink sink;
    runLoweredProgram(hopExpandCarryProgram, reader.getView(), sink, /*chunkSize=*/2);

    std::vector<CarriedHopSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<CarriedHopSink::Row> expected {{0, 1, 100},
                                                     {0, 2, 200},
                                                     {0, 2, 201},
                                                     {1, 4, 400}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, rejectsCarrySetOnProcedureNotDeclaringInputRowReporting) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // test.doubleNodeID emits a row per input row but declares no input-row report, so
    // its carried column could only be rebuilt by guessing. Refused while the call is
    // planned - nothing runs.
    EXPECT_THROW(lowerProgram(undeclaredCarryProgram, reader.getView()), IRException);
}

TEST_F(MLIRCallProcedureTest, rejectsCarrySetWhenDeclaredReportIsNotMade) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The declaration is the procedure's word, so a procedure that declares the report
    // and then does not make it gets past planning; the drive catches it at the first
    // chunk it emits rather than silently pairing the wrong rows.
    CarriedNodeSink sink;
    EXPECT_THROW(runLoweredProgram(brokenReportCarryProgram, reader.getView(), sink), IRException);
}

TEST_F(MLIRCallProcedureTest, rejectsCarrySetOnSourceCall) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    EXPECT_THROW(lowerProgram(sourceCarryProgram, reader.getView()), IRException);
}

TEST_F(MLIRCallProcedureTest, sourceCallCrossedWithAScan) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Five scanned nodes crossed with the call's two rows: ten pairs. The call's loop
    // opens inside the scan's, so the product can reach both factors' chunks; rooted at
    // function scope instead it would name a chunk that does not dominate it.
    NodePairSink sink;
    runLoweredProgram(crossedSourceCallProgram, reader.getView(), sink);

    std::vector<NodePairSink::Row> rows;
    sink.sortedRows(rows);

    std::vector<NodePairSink::Row> expected;
    for (uint64_t node = 0; node < 5; node++) {
        expected.emplace_back(node, 0);
        expected.emplace_back(node, 1);
    }

    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, sourceCallCrossedWithAMultiChunkScan) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The same product with the scan cut into three chunks: the call is the product's
    // inner factor, so its drive loop is re-entered once per outer chunk and rewound each
    // time. Every outer row still meets both of the call's rows, and none is emitted
    // twice - which only holds because the procedure restarts on the rewind.
    NodePairSink sink;
    runLoweredProgram(crossedSourceCallProgram, reader.getView(), sink, /*chunkSize=*/2);

    std::vector<NodePairSink::Row> rows;
    sink.sortedRows(rows);

    std::vector<NodePairSink::Row> expected;
    for (uint64_t node = 0; node < 5; node++) {
        expected.emplace_back(node, 0);
        expected.emplace_back(node, 1);
    }

    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherStandaloneCall) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A CALL with no MATCH: the call is the whole dataflow, so it opens it, and the YIELD
    // names the column the RETURN projects.
    NodeSink sink;
    runQuery("CALL test.twoNodes() YIELD id RETURN id", graph.get(), reader.getView(), sink);

    std::vector<uint64_t> rows;
    sink.sortedRows(rows);
    const std::vector<uint64_t> expected {0, 1};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherCallOverMatchedNodes) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n) CALL test.expandNodeID(n) YIELD copy RETURN n, copy - the whole path from
    // query text: the call takes the matched nodes as its argument, `n` rides through its
    // carry set, and the projection reads both. Nodes 1, 2 and 4 emit rows (`id % 3`), so
    // `n` is dropped for 0 and 3 and repeated for 2.
    CarriedNodeSink sink;
    runQuery("MATCH (n) CALL test.expandNodeID(n) YIELD copy RETURN n, copy", graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<CarriedNodeSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<CarriedNodeSink::Row> expected {{1, 100}, {2, 200}, {2, 201}, {4, 400}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherCallReadsANodeProperty) {
    auto graph = buildScoreGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n) CALL test.nodeScore(n) YIELD score RETURN n, score - a call whose rows come
    // out of the graph rather than out of its argument column: it resolves "score" against
    // the schema and reads the property off each matched node. Nodes 0, 1 and 3 carry one
    // (10, 20, 30), so those pair with their node; 2 and 4 carry none and are dropped.
    CarriedNodeSink sink;
    runQuery("MATCH (n) CALL test.nodeScore(n) YIELD score RETURN n, score", graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<CarriedNodeSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<CarriedNodeSink::Row> expected {{0, 10}, {1, 20}, {3, 30}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherCallReadsANodePropertyWithoutCarryingTheNode) {
    auto graph = buildScoreGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The same call projecting the property alone, so nothing is carried past it and the
    // procedure is handed no row map. The scores must still be the ones read from the
    // graph - the property read does not depend on the carry set.
    Int64Sink sink;
    runQuery("MATCH (n) CALL test.nodeScore(n) YIELD score RETURN score", graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<types::Int64::Primitive> rows = sink.rows();
    std::sort(rows.begin(), rows.end());
    const std::vector<types::Int64::Primitive> expected {10, 20, 30};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherCallReturnsAList) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n) CALL test.nodeIDList(n) YIELD values RETURN n, values - a call whose return
    // value is a LIST, so each row carries a whole list rather than one scalar. One row per
    // node, list length `id % 3`: nodes 0 and 3 get an empty list, 1 and 4 a single element,
    // 2 two of them. Lists exist only in the query language, so this is the one column type
    // a procedure produces that has no stored counterpart.
    CarriedListSink sink;
    runQuery("MATCH (n) CALL test.nodeIDList(n) YIELD values RETURN n, values", graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<CarriedListSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<CarriedListSink::Row> expected {
        {0, {}},
        {1, {100}},
        {2, {200, 201}},
        {3, {}},
        {4, {400}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherCallReturnsAStringListAndAnEmbedding) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n) CALL test.nodeTags(n) YIELD tags, coords RETURN n, tags, coords - two LIST
    // columns off one call: `tags` a list of strings (`id % 3` of them, empty for node 0 and
    // 3) and `coords` a list holding one embedding, {1.5 * id, 2.5 * id}. `vector` and
    // `embedding` are both reserved Cypher keywords, so neither can name a yield. Both element types
    // are stored in the buffer as a borrowed view - the characters and the floats live
    // outside it - so this covers the payload lifetime an Int64 element never exercises. An
    // embedding is not a ProcedureType, so a list element is the only way a call yields one.
    CarriedTagsSink sink;
    runQuery("MATCH (n) CALL test.nodeTags(n) YIELD tags, coords RETURN n, tags, coords",
             graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<CarriedTagsSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<CarriedTagsSink::Row> expected {
        {0, {}, {0.0f, 0.0f}},
        {1, {"alpha"}, {1.5f, 2.5f}},
        {2, {"alpha", "beta"}, {3.0f, 5.0f}},
        {3, {}, {4.5f, 7.5f}},
        {4, {"alpha"}, {6.0f, 10.0f}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherThreeChainedCallsWithoutMatch) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three calls and no MATCH, so the first call opens the dataflow and each one after it
    // reads what an earlier call yielded rather than what a traversal matched:
    //   test.twoNodes emits nodes 0 and 1
    //   test.expandNodeID(id) emits `id % 3` rows - nothing for 0, one row (copy 100) for 1
    //   test.fanOutNodeID(id) emits three rows per node - 10, 11, 12 for node 1
    // `id` therefore has to survive being carried past the second call to be the third's
    // argument, and `copy` past the third to be projected, so this is the one case where a
    // yielded column is carried through more than one call.
    ChainedCallSink sink;
    runQuery("CALL test.twoNodes() YIELD id "
             "CALL test.expandNodeID(id) YIELD copy "
             "CALL test.fanOutNodeID(id) YIELD value "
             "RETURN id, copy, value",
             graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<ChainedCallSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<ChainedCallSink::Row> expected {{1, 100, 10}, {1, 100, 11}, {1, 100, 12}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherThreeUncorrelatedCallsWithoutMatch) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three calls, none reading what another produced and none reading a match, so each one
    // is a source and the three are crossed: two nested db.cross_products rather than the
    // argument chain. test.twoNodes emits nodes 0 and 1, so the result is the whole 2 x 2 x 2
    // product. Calling the one procedure three times also pins that each CALL is its own
    // call - three nl.procedure handles, each with its own state, rather than one CSE'd
    // handle driven three times, which would emit a single node per row instead.
    NodeTripleSink sink;
    runQuery("CALL test.twoNodes() YIELD id AS a "
             "CALL test.twoNodes() YIELD id AS b "
             "CALL test.twoNodes() YIELD id AS c "
             "RETURN a, b, c",
             graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<NodeTripleSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<NodeTripleSink::Row> expected {
        {0, 0, 0},
        {0, 0, 1},
        {0, 1, 0},
        {0, 1, 1},
        {1, 0, 0},
        {1, 0, 1},
        {1, 1, 0},
        {1, 1, 1},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherUncorrelatedCallCrossesWithTheMatch) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The call reads none of the matched rows, so it produces the same two rows for every
    // one of them: the five matched nodes crossed with them, ten pairs. The generator
    // moves the traversal into one factor of a cross product and the call into the other.
    NodePairSink sink;
    runQuery("MATCH (n) CALL test.twoNodes() YIELD id RETURN n, id", graph.get(), reader.getView(), sink);

    std::vector<NodePairSink::Row> rows;
    sink.sortedRows(rows);

    std::vector<NodePairSink::Row> expected;
    for (uint64_t node = 0; node < 5; node++) {
        expected.emplace_back(node, 0);
        expected.emplace_back(node, 1);
    }

    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherTwoCallsOverAHopProjectingBothYields) {
    auto graph = buildHopGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n)-->(m) CALL f(n) YIELD x CALL g(m) YIELD y RETURN x + y: each call carries
    // everything else in flight, so the second still reads the `m` the hop bound and the
    // `x` the first call yielded, both replicated against the rows it emits.
    //
    // Over the hop pairs (0,1), (0,2), (1,4) and (2,3): test.expandNodeID emits `n % 3`
    // rows, so the pairs with n=0 drop out and (1,4) gives x=100 while (2,3) gives x=200 and
    // x=201. test.fanOutNodeID then emits three rows per surviving `m` - 10*m, 10*m+1,
    // 10*m+2 - so m=4 pairs 100 with 40, 41 and 42, and each x from m=3 pairs with 30, 31
    // and 32.
    Int64Sink sink;
    runQuery("MATCH (n)-->(m) "
             "CALL test.expandNodeID(n) YIELD copy AS x "
             "CALL test.fanOutNodeID(m) YIELD value AS y "
             "RETURN x + y",
             graph.get(),
             reader.getView(),
             sink);

    std::vector<types::Int64::Primitive> rows = sink.rows();
    std::sort(rows.begin(), rows.end());
    const std::vector<types::Int64::Primitive> expected {140, 141, 142, 230, 231, 231, 232, 232, 233};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherYieldWhereKeepsAliveWhatOnlyItReads) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The second call's YIELD ... WHERE is the only reader of `copy` - the projection returns
    // `value` alone - so `copy` has to survive the second call for the filter to see it.
    // test.expandNodeID leaves rows with copy 100, 200, 201 and 400 (for n = 1, 2, 2, 4);
    // test.fanOutNodeID then emits 10*n, 10*n+1 and 10*n+2 for each, and the filter drops the
    // three rows carrying copy=100.
    Int64Sink sink;
    runQuery("MATCH (n) CALL test.expandNodeID(n) YIELD copy "
             "CALL test.fanOutNodeID(n) YIELD value WHERE copy > 100 RETURN value",
             graph.get(),
             reader.getView(),
             sink);

    std::vector<types::Int64::Primitive> rows = sink.rows();
    std::sort(rows.begin(), rows.end());
    const std::vector<types::Int64::Primitive> expected {20, 20, 21, 21, 22, 22, 40, 41, 42};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherCallStillRejectsCarryingPastANonReportingProcedure) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // `n` is in flight, so it rides through the carry set - and test.doubleNodeID cannot say
    // which of its rows came from which input row, so the call is refused.
    CarriedNodeSink sink;
    EXPECT_THROW(runQuery("MATCH (n) CALL test.doubleNodeID(n) YIELD doubled RETURN n, doubled",
                          graph.get(),
                          reader.getView(),
                          sink),
                 IRException);
}

TEST_F(MLIRCallProcedureTest, cypherCallWithoutYieldEmitsEveryReturnValue) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A standalone CALL names no return value, so it emits every one db.labels declares -
    // its id and its label - in the order the procedure declares them.
    LabelSink sink;
    runQuery("CALL db.labels()", graph.get(), reader.getView(), sink);

    std::vector<LabelSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<LabelSink::Row> expected {{0, "Person"},
                                                {1, "Employee"},
                                                {2, "Manager"},
                                                {3, "Contractor"},
                                                {4, "Intern"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherCallOfProcedureWithNoResults) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A procedure declaring no return value is called for what it does: the query is the
    // drive and nothing is emitted, so the call binds no column and no output is
    // generated at all.
    sideEffectCalls = 0;

    CountingSink sink;
    runQuery("CALL test.sideEffect()", graph.get(), reader.getView(), sink);

    EXPECT_EQ(sideEffectCalls, 1u);
    EXPECT_EQ(sink.getCalls(), 0u);
}

TEST_F(MLIRCallProcedureTest, cypherCrossedCallYieldsNonIdColumns) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n) CALL db.labels() YIELD label RETURN n, label - the call is crossed with the
    // match, and what it yields is a borrowed-string column rather than an ID one, so the
    // product has to broadcast a chunk kind only a procedure produces. Five nodes by five
    // labels: twenty-five rows, each label against each node.
    // Three chunks of nodes, so the product re-enters the call's drive twice over and the
    // label scan is rewound each time - every node still meets every label, exactly once.
    NodeLabelSink sink;
    runQuery("MATCH (n) CALL db.labels() YIELD label RETURN n, label", graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<NodeLabelSink::Row> rows;
    sink.sortedRows(rows);

    std::vector<NodeLabelSink::Row> expected;
    const std::vector<std::string> labels {"Person", "Employee", "Manager", "Contractor", "Intern"};
    for (uint64_t node = 0; node < 5; node++) {
        for (const std::string& label : labels) {
            expected.emplace_back(node, label);
        }
    }
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRCallProcedureTest, cypherCrossedCallRewindsItsCursor) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // db.history walks a cursor down the commit chain, consuming it, so a drive re-entered
    // by the product only produces rows if the rewind puts the cursor back. Take the
    // commit count from a standalone call, then require the crossed one to pair every
    // commit with every node - a stale cursor would leave the later chunks empty.
    CountingSink standaloneSink;
    runQuery("CALL db.history() YIELD nodeCount RETURN nodeCount", graph.get(), reader.getView(), standaloneSink);

    const size_t commitCount = standaloneSink.getRows();
    ASSERT_GT(commitCount, 0u);

    CountingSink crossedSink;
    runQuery("MATCH (n) CALL db.history() YIELD nodeCount RETURN n, nodeCount", graph.get(),
             reader.getView(),
             crossedSink,
             /*chunkSize=*/2);

    EXPECT_EQ(crossedSink.getRows(), 5 * commitCount);
}

TEST_F(MLIRCallProcedureTest, rejectsUnknownProcedure) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    EXPECT_THROW(lowerProgram(unknownProcedureProgram, reader.getView()), IRException);
}

TEST_F(MLIRCallProcedureTest, rejectsUnknownYield) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The yielded name must be one of the procedure's declared return values; the
    // registry rejects it with a ProcedureException, which derives from TuringException.
    EXPECT_THROW(lowerProgram(unknownYieldProgram, reader.getView()), TuringException);
}

TEST_F(MLIRCallProcedureTest, rejectsArgumentCountMismatch) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    EXPECT_THROW(lowerProgram(tooManyArgumentsProgram, reader.getView()), IRException);
}

TEST_F(MLIRCallProcedureTest, dbCallProcedureRoundTrips) {
    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();

    const mlir::ParserConfig parserConfig(&context);
    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(labelsProgram, parserConfig);
    ASSERT_TRUE(module);

    mlir::func::FuncOp function = module->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(function);

    mlir::db::CallProcedure call;
    function.walk([&](mlir::db::CallProcedure parsed) { call = parsed; });
    ASSERT_TRUE(call);

    EXPECT_EQ(call.getProcedure(), "db.labels");
    EXPECT_EQ(call.getInputs().size(), 0u);
    EXPECT_EQ(call.getYields().size(), 2u);
    EXPECT_EQ(call.getResults().size(), 2u);
}

TEST_F(MLIRCallProcedureTest, nlProcedureOpsRoundTrip) {
    constexpr const char* nlProgram = R"mlir(
func.func @main() {
  %call = nl.procedure("db.labels") yields ["id", "label"]
  %rows = nl.procedure_init(%call, (), {}) : (!nl.procedure_state) -> !nl.iter<!nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>>
  nl.for %ids, %labels in %rows : !nl.iter<!nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>> {
    nl.output(%ids, %labels) : !nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>
  }
  return
}
)mlir";

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(nlProgram, parserConfig);
    ASSERT_TRUE(module);

    mlir::func::FuncOp function = module->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(function);

    mlir::nl::Procedure procedureOp;
    function.walk([&](mlir::nl::Procedure parsed) { procedureOp = parsed; });
    ASSERT_TRUE(procedureOp);

    EXPECT_EQ(procedureOp.getName(), "db.labels");
    EXPECT_EQ(procedureOp.getYields().size(), 2u);
}

TEST_F(MLIRCallProcedureTest, cypherCallSamplesANeighbourhood) {
    auto graph = buildHopGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // gnn.neighbourhoodSample is the registered procedure that reports the input row of
    // the rows it emits, so this is the carry set over a real one rather than a
    // test-local stand-in. It also reads its argument column in its prepare step, which
    // is why the call is prepared on its first drive. A sample size above the out-degree
    // of every node takes all four edges, making the rows deterministic.
    NodePairSink sink;
    runQuery("MATCH (n) CALL gnn.neighbourhoodSample(n, 2, 42) YIELD tgt RETURN n, tgt",
             graph.get(),
             reader.getView(),
             sink);

    std::vector<NodePairSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<NodePairSink::Row> expected {{0, 1}, {0, 2}, {1, 4}, {2, 3}};
    EXPECT_EQ(rows, expected);
}
