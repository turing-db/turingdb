#include "Frame.h"

#include "SymbolTable.h"

using namespace db::query;

Frame::Frame(SymbolTable* symTbl)
    : _symTbl(symTbl),
    _tbl(symTbl->size())
{
}

Frame::~Frame() {
}
