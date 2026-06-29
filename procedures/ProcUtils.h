#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "ProcedureException.h"
#include "columns/ColumnConst.h"

namespace db {

class EntityPropertyView;
class PropertyTypeMap;
class ListView;

// Static helpers shared by the data-fetching procedures (db.listNodes,
// db.getEdges, db.getNodes, ...): reading list-literal arguments and encoding
// entity properties into STRING return columns.
class ProcUtils {
public:
    // Append `s` to `out` as an escaped JSON string literal (surrounding quotes
    // included).
    static void appendJsonString(std::string& out, std::string_view s);

    // Encode an entity's properties into `out` as a JSON object of typed values,
    // keyed by property NAME (properties with no name are skipped). Used for both
    // node and edge properties so the visualiser sees one consistent shape. `out`
    // is cleared first, so a single buffer can be reused across rows.
    static void encodeProperties(const EntityPropertyView& props,
                                 const PropertyTypeMap& propTypes,
                                 std::string& out);

    // Read the integer elements of `view` into `out` (`out` is cleared first);
    // non-integer elements are skipped. The caller supplies the list view,
    // typically via `constArg<ListView>`, which enforces the constant-list rule.
    static void readIntList(const ListView* view, std::vector<int64_t>& out);

    // Return the value of a constant-column argument, throwing `errorMsg` (as a
    // ProcedureException) if `arg` isn't exactly a `ColumnConst<T>`. The analyzer
    // guarantees the argument's type; this enforces the *constant* shape the
    // data-fetching procedures rely on (each argument is read once, not per row).
    template <typename T>
    static const T& constArg(const Column* arg, std::string_view errorMsg) {
        if (!arg || arg->getKind() != ColumnConst<T>::staticKind()) {
            throw ProcedureException(std::string(errorMsg));
        }
        return static_cast<const ColumnConst<T>*>(arg)->getRaw();
    }
};

}
