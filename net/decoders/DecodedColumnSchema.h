#pragma once

#include <stddef.h>
#include <string>

#include "Bitmask.h"
#include "TuringProtoHeaders.h"

namespace net::proto {

// Per-column decode state carried across chunk and buffer boundaries
class ProtoColumnState {
public:
    using BitMask = DynamicLargeBitMask<uint64_t>;
    ProtoColumnState();
    ProtoColumnState(const ProtoColumnState&);
    ProtoColumnState(ProtoColumnState&&);
    ProtoColumnState& operator=(const ProtoColumnState&);
    ProtoColumnState& operator=(ProtoColumnState&&);
    ~ProtoColumnState();

    BitMask& getBitMask() { return _bitMask; }
    const BitMask& getBitMask() const { return _bitMask; }

    WireSize getNumRows() const { return _numRows; }
    void setNumRows(WireSize numRows) { _numRows = numRows; }

    void reset() {
        _bitMask.resize(0);
        _numRows = 0;
    }

private:
    // The bitmask prefixing an optional column's wire encoding. We need to keep its state
    // across multiple chunks if needed.
    DynamicLargeBitMask<uint64_t> _bitMask {0};
    // The total number of rows that prefixes every column's wire bytes
    WireSize _numRows {0};
};

// One decoded column's schema. The decoder builds a vector of these from the chunk header 
// and fills the state as each chunk's data arrives.
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

    const std::string& getColumnName() const { return _columnName; }
    void setColumnName(const std::string& columnName) { _columnName = columnName; }

    ProtoColumnState& getColumnState() { return _columnState; }
    const ProtoColumnState& getColumnState() const { return _columnState; }

private:
    ColumnWireHeader _header;
    std::string _columnName;
    ProtoColumnState _columnState;
};

}
