#pragma once

#include <stddef.h>
#include <string>

#include "Bitmask.h"
#include "TuringProtoHeaders.h"

namespace net::proto {

// Per-column decode state carried across chunk and buffer boundaries: the null
// bitmask and how many rows the column contains.
class ProtoColumnState {
public:
    ProtoColumnState();
    ProtoColumnState(const ProtoColumnState&);
    ProtoColumnState(ProtoColumnState&&);
    ProtoColumnState& operator=(const ProtoColumnState&);
    ProtoColumnState& operator=(ProtoColumnState&&);
    ~ProtoColumnState();

    DynamicLargeBitMask<uint64_t>& getBitMask() { return _bitMask; }
    const DynamicLargeBitMask<uint64_t>& getBitMask() const { return _bitMask; }

    WireSize getNumRows() const { return _numRows; }
    void setNumRows(WireSize numRows) { _numRows = numRows; }

    void reset() {
        _bitMask.resize(0);
        _numRows = 0;
    }

private:
    // The bitmask prefixing an optional column's wire encoding. We need to keep it's state
    // across multiple chunks if needed.
    DynamicLargeBitMask<uint64_t> _bitMask {0};
    // The total numbers of rows that prepends every column's wire bytes
    WireSize _numRows {0};
};

// One decoded column's schema: its wire header, its name, and the wire columns metadata.
// The decoder builds a vector of these from the chunk header and fills the state as each
// chunk's data arrives.
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

    ProtoColumnState& getColumnState() { return _columnState; }
    const ProtoColumnState& getColumnState() const { return _columnState; }

private:
    ColumnWireHeader _header;
    std::string _colName;
    ProtoColumnState _columnState;
};

}
