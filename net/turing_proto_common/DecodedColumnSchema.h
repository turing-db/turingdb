#pragma once

#include <stddef.h>
#include <string>

#include "Bitmask.h"
#include "TuringProtoHeaders.h"

namespace net::proto {

// Per-column decode state carried across chunk and buffer boundaries: the null
// bitmask built up so far and how many rows have been written into the column.
class DfColumnState {
public:
    DfColumnState();
    DfColumnState(const DfColumnState&);
    DfColumnState(DfColumnState&&);
    DfColumnState& operator=(const DfColumnState&);
    DfColumnState& operator=(DfColumnState&&);
    ~DfColumnState();

    DynamicLargeBitMask<uint64_t>& getBitMask() { return _bitMask; }
    const DynamicLargeBitMask<uint64_t>& getBitMask() const { return _bitMask; }

    WireSize getNumRows() const { return _numRows; }
    void setNumRows(WireSize numRows) { _numRows = numRows; }

    void reset() {
        _bitMask.resize(0);
        _numRows = 0;
    }

private:
    DynamicLargeBitMask<uint64_t> _bitMask {0};
    WireSize _numRows {0};
};

// One decoded column's schema: its wire header, its name, and the running decode
// state. The decoder builds a vector of these from the chunk header and fills the
// state as each chunk's data arrives.
class DecodedColumnSchema {
public:
    DecodedColumnSchema();
    DecodedColumnSchema(const DecodedColumnSchema&);
    DecodedColumnSchema(DecodedColumnSchema&&);
    DecodedColumnSchema& operator=(const DecodedColumnSchema&);
    DecodedColumnSchema& operator=(DecodedColumnSchema&&);
    ~DecodedColumnSchema();

    ColumnWireHeader& getHeader() { return _header; }
    const ColumnWireHeader& getHeader() const { return _header; }

    const std::string& getColName() const { return _colName; }
    void setColName(const std::string& colName) { _colName = colName; }

    DfColumnState& getColState() { return _colState; }
    const DfColumnState& getColState() const { return _colState; }

private:
    ColumnWireHeader _header;
    std::string _colName;
    DfColumnState _colState;
};

}
