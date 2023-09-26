#include "Frame.h"

#include "SymbolTable.h"

using namespace db;

Frame::Frame(SymbolTable* symTbl)
    : _symTbl(symTbl),
    _tbl(symTbl->size())
{
}

Frame::~Frame() {
}
