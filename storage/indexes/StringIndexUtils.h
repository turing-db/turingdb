#pragma once

#include <concepts>

#include "DataPart.h"
#include "DataPartSpan.h"
#include "ID.h"
#include "TuringException.h"
#include "indexers/StringPropertyIndexer.h"
#include "views/GraphView.h"

namespace db {

class GraphView;
class StringIndex;

class StringIndexUtils {
public:
    // NOTE: Better to do lookup table?
    static size_t charToIndex(char c) {
        // Children array layout:
        // INDEX CHARACTER VALUE
        // 0     a
        // ...  ...
        // 25    z
        // 26    0
        // ...  ...
        // 36    9

        // NOTE: Converts upper-case characters to lower to calculate index,
        // but the value of the node is still uppercase
        if (isalpha(c)) {
            return std::tolower(c, std::locale()) - 'a';
        } else if (isdigit(c)) {
            return 26 + c - '0';
        } else
            throw TuringException("Invalid character: " + std::to_string(c));
    }
    static inline char indexToChar(size_t idx) {
        char c = idx < 26 ? 'a' + idx : '0' + idx - 26;
        return c;
    }

    /*
     * @brief Fills @param ouput with matches on the query @param query in the indexes for
     * all dataparts
     * @warn Clears @param output on entry
     * @param view The graphview to retrieve dataparts from
     * @param propType The property type to query against
     * @param query The string to query against
     * @throws TuringException if any datapart member of @ref view has an uninitialised
     * string approximator index
     */
    template <TypedInternalID IDT>
    static void getMatches(std::vector<IDT>& output, const GraphView& view,
                           PropertyTypeID propID, const std::string& query) {
        const auto& dps = view.dataparts();
        output.clear();

        if constexpr (std::same_as<IDT, NodeID>) {
            // For each datapart
            for (DataPartIterator it = dps.begin(); it != dps.end(); it++) {
                // Get PropertyID -> Index map
                const auto& nodeStringIndex = it->get()->getNodeStrPropIndexer();

                if (!nodeStringIndex.isInitialised()) {
                    throw TuringException(
                        "Approximate string index was not initialised. The current graph "
                        "was likely loaded incorrectly.");
                }


                // Check if the datapart contains an index of this property ID
                if (!nodeStringIndex.contains(propID)) {
                    continue;
                }

                // Get the index for this property
                const auto& strIndex = nodeStringIndex.at(propID);

                // Get any matches for the query string in the index
                strIndex->query<IDT>(output, query);
            }
        } else if constexpr (std::same_as<IDT, EdgeID>) {
            // For each datapart
            for (DataPartIterator it = dps.begin(); it != dps.end(); it++) {
                // Get PropertyID -> Index map
                const auto& edgeStringIndex = it->get()->getEdgeStrPropIndexer();
                if (!edgeStringIndex.isInitialised()) {
                    throw TuringException(
                        "Approximate string index was not initialised. The current graph "
                        "was likely loaded incorrectly.");
                }

                // Check if the datapart contains an index of this property ID
                if (!edgeStringIndex.contains(propID)) {
                    continue;
                }

                // Get the index for this property
                const auto& strIndex = edgeStringIndex.at(propID);

                // Get any matches for the query string in the index
                strIndex->query<IDT>(output, query);
            }
        }
    }
};
}
