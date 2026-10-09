#pragma once

#include <memory>

#include "buffers/StringBuffer.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"

namespace db {

// The buffers one op builds the strings, lists and maps of a run in. They are emptied
// when the op runs again, so they hold one run's values rather than every row the query
// evaluated: an op keeping values longer copies them into an NLValueStore of its own.
// Most ops build none, so each buffer is created on first use.
class NLStepBuffers {
public:
    NLStepBuffers();
    ~NLStepBuffers();

    QueryListBuffer& listBuffer();
    MapBuffer<>& mapBuffer();
    StringBuffer& stringBuffer();

    void clear();

private:
    std::unique_ptr<QueryListBuffer> _lists;
    std::unique_ptr<MapBuffer<>> _maps;
    std::unique_ptr<StringBuffer> _strings;
};

}
