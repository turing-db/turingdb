#include "NLStepBuffers.h"

using namespace db;

NLStepBuffers::NLStepBuffers()
{
}

NLStepBuffers::~NLStepBuffers() {
}

QueryListBuffer& NLStepBuffers::listBuffer() {
    if (!_lists) {
        _lists = std::make_unique<QueryListBuffer>();
    }

    return *_lists;
}

MapBuffer<>& NLStepBuffers::mapBuffer() {
    if (!_maps) {
        _maps = std::make_unique<MapBuffer<>>();
    }

    return *_maps;
}

StringBuffer& NLStepBuffers::stringBuffer() {
    if (!_strings) {
        _strings = std::make_unique<StringBuffer>();
    }

    return *_strings;
}

void NLStepBuffers::clear() {
    if (_lists) {
        _lists->clear();
    }

    if (_maps) {
        _maps->clear();
    }

    if (_strings) {
        _strings->clear();
    }
}
