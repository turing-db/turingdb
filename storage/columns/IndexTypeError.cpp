#include "IndexTypeError.h"

#include <string_view>

#include <spdlog/fmt/bundled/format.h>

#include "QueryTypeName.h"

#include "TuringException.h"

using namespace db;

void db::throwIndexTypeError(CellTag base, CellTag index) {
    const std::string_view indexName = queryTypeName(index);

    if (base == CellTag::MapView) {
        throw TuringException(fmt::format("A map is indexed by a string key, not by '{}'", indexName));
    } else if (base == CellTag::NodeID) {
        throw TuringException(fmt::format("A node is indexed by a string key, not by '{}'", indexName));
    } else if (base == CellTag::EdgeID) {
        throw TuringException(fmt::format("A relationship is indexed by a string key, not by '{}'", indexName));
    } else if (base == CellTag::ListView) {
        throw TuringException(fmt::format("A list index expression must be an integer, not '{}'", indexName));
    }

    throw TuringException(fmt::format("Index operator [] can only be applied to a list, a map, a node or a relationship, not '{}'",
                                      queryTypeName(base)));
}
