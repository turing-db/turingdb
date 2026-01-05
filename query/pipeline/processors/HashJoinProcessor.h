#pragma once

#include <unordered_map>

#include "Processor.h"
#include "RowStore.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "ID.h"

namespace db {
class ColumnTag;
}

namespace db {
using HashJoinMap = std::unordered_map<NodeID, std::vector<RowOffset>>;

struct RowOffsetsCopyState {
    const Dataframe* _df {nullptr};
    const std::vector<RowOffset>* _offsetVec {nullptr};
    size_t _rowOffsetIdx {0};

    // If we have a df pointer there is some
    //  uncopied RowOffsets
    bool hasRowOffsets() { return _df != nullptr; };

    void reset() {
        _df = nullptr;
        _offsetVec = nullptr;
        _rowOffsetIdx = 0;
    }

    void initialise(const Dataframe* df,
                    const std::vector<RowOffset>* offsetVec,
                    size_t rowOffsetIdx) {
        _df = df;
        _offsetVec = offsetVec;
        _rowOffsetIdx = rowOffsetIdx;
    }

    void incrementRowOffsetIdx(size_t increment) { _rowOffsetIdx += increment; };

    size_t numRemainingOffsets() { return _offsetVec->size() - _rowOffsetIdx; };
};

class HashJoinProcessor : public Processor {
public:
    static HashJoinProcessor* create(PipelineV2* pipeline,
                                     ColumnTag leftJoinKey,
                                     ColumnTag rightJoinKey);

    PipelineBlockInputInterface& rightInput() { return _rightInput; }
    PipelineBlockInputInterface& leftInput() { return _leftInput; }
    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    std::string describe() const final;

private:
    PipelineBlockInputInterface _rightInput;
    PipelineBlockInputInterface _leftInput;
    PipelineBlockOutputInterface _output;

    // These are the length of the rows we are storing
    size_t _leftRowLen {0};
    size_t _rightRowLen {0};

    // map to a vector of offsets
    // the offsets point to the corresponding row in our
    HashJoinMap _rightMap;
    HashJoinMap _leftMap;

    RowStore _store;

    ColumnTag _leftJoinKey;
    ColumnTag _rightJoinKey;

    // These indexes help us keep track of where to start iterating on the columns
    // of the input
    size_t _leftInputIdx {0};
    size_t _rightInputIdx {0};

    bool _hasWritten {false};

    // Stores Information To Finish Off A Join
    // Mid RowOffset Vector.
    RowOffsetsCopyState _rowOffsetState;

    void processRightStream(size_t& rowsRemaining, size_t& totalRowsInserted);
    void processLeftStream(size_t& rowsRemaining, size_t& totalRowsInserted);

    void flushRightStream(size_t& rowsRemaining, size_t& totalRowsInserted);
    void flushLeftStream(size_t& rowsRemaining, size_t& totalRowsInserted);

    HashJoinProcessor(ColumnTag leftJoinKey, ColumnTag rightJoinKey);
    ~HashJoinProcessor() override = default;
};

}
