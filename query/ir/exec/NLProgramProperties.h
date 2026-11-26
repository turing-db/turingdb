#pragma once

#include <stdint.h>
#include <unordered_set>

#include "ID.h"
#include "NLValueStore.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"

namespace db {

// nl.get_node_properties / nl.get_edge_properties data: a with-null property
// read that maps the input ID column to a nullable value column, one value per
// input row (missing values are null, no row dropped). The node-vs-edge ID type
// and the value type are baked into the chosen handler; the PropertyTypeID was
// resolved from the name against the schema during translation.
class NLPropertyFetchData : public NLFunctionData {
public:
    NLPropertyFetchData(const Column* input, Column* output, PropertyTypeID propertyTypeID)
        : _input(input),
        _output(output),
        _propertyTypeID(propertyTypeID)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }

    // The rows naming an entity this change wrote, by the ID it will commit as, or null
    // for an input no merge produced. Such a row reads its value out of the write buffer.
    // A column a create produced is such a row throughout, which is what _allPending says.
    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    // The values this change wrote that a run fetched, copied in and emptied at the next run
    NLValueStore& fetchedValues() { return _fetchedValues; }

private:
    const Column* _input {nullptr};
    Column* _output {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    PropertyTypeID _propertyTypeID;
    bool _allPending {false};
    NLValueStore _fetchedValues;
};

class NLGetNodeLabelSetData : public NLFunctionData {
public:
    NLGetNodeLabelSetData(const ColumnNodeIDs* input, ColumnLabelSetIDs* output)
        : _input(input),
        _output(output)
    {
    }

    const ColumnNodeIDs* getInput() const { return _input; }
    ColumnLabelSetIDs* getOutput() const { return _output; }

private:
    const ColumnNodeIDs* _input {nullptr};
    ColumnLabelSetIDs* _output {nullptr};
};

class NLGetEdgeTypesData : public NLFunctionData {
public:
    NLGetEdgeTypesData(const ColumnEdgeIDs* input, ColumnEdgeTypes* output)
        : _input(input),
        _output(output)
    {
    }

    const ColumnEdgeIDs* getInput() const { return _input; }
    ColumnEdgeTypes* getOutput() const { return _output; }

private:
    const ColumnEdgeIDs* _input {nullptr};
    ColumnEdgeTypes* _output {nullptr};
};

class NLCheckLabelConstraintData : public NLFunctionData {
public:
    NLCheckLabelConstraintData(const ColumnLabelSetIDs* input, Column* output)
        : _input(input),
        _output(output)
    {
    }

    const ColumnLabelSetIDs* getInput() const { return _input; }
    Column* getOutput() const { return _output; }

    void addMatchingID(LabelSetID id) { _matchingIDs.insert(id.getValue()); }
    bool isMatching(LabelSetID id) const { return _matchingIDs.count(id.getValue()) > 0; }

private:
    const ColumnLabelSetIDs* _input {nullptr};
    Column* _output {nullptr};
    std::unordered_set<uint32_t> _matchingIDs;
};

class NLCheckEdgeTypeConstraintData : public NLFunctionData {
public:
    NLCheckEdgeTypeConstraintData(const ColumnEdgeTypes* input, Column* output)
        : _input(input),
        _output(output)
    {
    }

    const ColumnEdgeTypes* getInput() const { return _input; }
    Column* getOutput() const { return _output; }

    void addMatchingID(EdgeTypeID id) { _matchingIDs.insert(id.getValue()); }
    bool isMatching(EdgeTypeID id) const { return _matchingIDs.count(id.getValue()) > 0; }

private:
    const ColumnEdgeTypes* _input {nullptr};
    Column* _output {nullptr};
    std::unordered_set<uint64_t> _matchingIDs;
};

}
