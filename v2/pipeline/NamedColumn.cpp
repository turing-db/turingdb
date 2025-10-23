#include "NamedColumn.h"

#include "PipelineBlock.h"

using namespace db::v2;

NamedColumn::NamedColumn(PipelineBlock* block,
                         Column* col,
                         std::string_view name)
    : _parent(block),
    _col(col),
    _name(name)
{
}

NamedColumn::~NamedColumn() {
}

NamedColumn* NamedColumn::create(PipelineBlock* block, Column* col, std::string_view name) {
    NamedColumn* namedCol = new NamedColumn(block, col, name);
    block->addColumn(namedCol);
    return namedCol;
}
