#include "PipelineBlock.h"

#include "NamedColumn.h"

using namespace db::v2;

PipelineBlock::PipelineBlock()
{
}

PipelineBlock::~PipelineBlock() {
    for (NamedColumn* namedCol : _cols) {
        delete namedCol;
    }
}

void PipelineBlock::addColumn(NamedColumn* col) {
    _cols.push_back(col);
}
