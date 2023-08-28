#include "QueryValue.h"

#include "DBService.pb.h"

using namespace turing::db::client;

std::string QueryValue::toString() const {
    if (_cell.has_string()) {
        return _cell.string();
    } else if (_cell.has_int64()) {
        return std::to_string(_cell.int64());
    } else if (_cell.has_uint64()) {
        return std::to_string(_cell.uint64());
    } else if (_cell.has_boolean()) {
        return std::to_string(_cell.boolean());
    } else if (_cell.has_decimal()) {
        return std::to_string(_cell.decimal());
    }

    return "";
}
