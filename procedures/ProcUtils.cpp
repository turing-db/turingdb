#include "ProcUtils.h"

#include <algorithm>
#include <cmath>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "views/EntityPropertyView.h"
#include "views/PropertyView.h"
#include "metadata/PropertyTypeMap.h"
#include "metadata/PropertyType.h"
#include "list/ListView.h"
#include "CellView.h"
#include "CellTag.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "ID.h"

using namespace db;

namespace {

void appendListValue(std::string& out, ListView list);
void appendMapValue(std::string& out, MapView map);

// Append a double as JSON; there are no nan/inf tokens, so those become null.
void appendDouble(std::string& out, double value) {
    if (std::isfinite(value)) {
        out += fmt::format("{}", value);
    } else {
        out += "null";
    }
}

// Append a single element of a list as its typed JSON representation.
void appendCell(std::string& out, CellView element) {
    switch (element.getTag()) {
        case CellTag::Int:
            out += fmt::format("{}", element.getAs<types::Int64::Primitive>());
        break;
        case CellTag::UInt:
            out += fmt::format("{}", element.getAs<types::UInt64::Primitive>());
        break;
        case CellTag::Double:
            appendDouble(out, element.getAs<types::Double::Primitive>());
        break;
        case CellTag::Bool:
            out += (static_cast<bool>(element.getAs<types::Bool::Primitive>()) ? "true" : "false");
        break;
        case CellTag::String:
            ProcUtils::appendJsonString(out, element.getAs<types::String::Primitive>());
        break;
        case CellTag::Embedding: {
            out += '[';
            bool firstElem = true;
            for (const float f : element.getAs<types::Embedding::Primitive>()) {
                if (!firstElem) {
                    out += ',';
                }
                firstElem = false;
                appendDouble(out, f);
            }
            out += ']';
        }
        break;
        case CellTag::ListView:
            appendListValue(out, element.getAs<ListView>());
        break;
        case CellTag::NodeID:
            out += fmt::format("{}", element.getAs<NodeID>().getValue());
        break;
        case CellTag::EdgeID:
            out += fmt::format("{}", element.getAs<EdgeID>().getValue());
        break;
        case CellTag::DateTime: {
            std::string formatted;
            DateTime::format(formatted, element.getAs<types::DateTime::Primitive>());

            ProcUtils::appendJsonString(out, formatted);
        }
        break;
        case CellTag::Duration: {
            std::string formatted;
            Duration::format(formatted, element.getAs<types::Duration::Primitive>());

            ProcUtils::appendJsonString(out, formatted);
        }
        break;
        case CellTag::MapView:
            appendMapValue(out, element.getAs<MapView>());
        break;
        case CellTag::Null:
        case CellTag::INVALID:
            out += "null";
        break;
    }
}

// Append a list as a JSON array of its elements.
void appendListValue(std::string& out, ListView list) {
    out += '[';

    bool firstElem = true;
    for (const CellView element : list) {
        if (!firstElem) {
            out += ',';
        }
        firstElem = false;
        appendCell(out, element);
    }

    out += ']';
}

// Append a map as a JSON object of its entries.
void appendMapValue(std::string& out, MapView map) {
    out += '{';

    bool firstEntry = true;
    for (const MapEntryView entry : map) {
        if (!firstEntry) {
            out += ',';
        }
        firstEntry = false;
        ProcUtils::appendJsonString(out, entry.getKey());
        out += ':';
        appendCell(out, entry.getValue());
    }

    out += '}';
}

// Append a single property value as its typed JSON representation.
void appendPropertyValue(std::string& out, const PropertyVariant& value) {
    std::visit(
        [&](const auto* ptr) {
            using V = std::remove_cvref_t<decltype(*ptr)>;
            if (!ptr) {
                out += "null";
            } else if constexpr (std::is_same_v<V, std::string_view>) {
                ProcUtils::appendJsonString(out, *ptr);
            } else if constexpr (std::is_same_v<V, CustomBool>) {
                out += (static_cast<bool>(*ptr) ? "true" : "false");
            } else if constexpr (std::is_same_v<V, ListView>) {
                appendListValue(out, *ptr);
            } else if constexpr (std::is_same_v<V, DateTime>) {
                std::string formatted;
                DateTime::format(formatted, *ptr);

                ProcUtils::appendJsonString(out, formatted);
            } else if constexpr (std::is_same_v<V, Duration>) {
                std::string formatted;
                Duration::format(formatted, *ptr);

                ProcUtils::appendJsonString(out, formatted);
            } else if constexpr (std::is_same_v<V, MapView>) {
                appendMapValue(out, *ptr);
            } else if constexpr (std::is_same_v<V, std::span<const float>>) {
                out += '[';
                bool firstElem = true;
                for (const float f : *ptr) {
                    if (!firstElem) {
                        out += ',';
                    }
                    firstElem = false;
                    // JSON has no nan/inf tokens; emit null for non-finite values.
                    if (std::isfinite(f)) {
                        out += fmt::format("{}", f);
                    } else {
                        out += "null";
                    }
                }
                out += ']';
            } else if constexpr (std::is_floating_point_v<V>) {
                // JSON has no nan/inf tokens; emit null for non-finite doubles so
                // the visualiser's JSON.parse doesn't choke.
                if (std::isfinite(*ptr)) {
                    out += fmt::format("{}", *ptr);
                } else {
                    out += "null";
                }
            } else {
                out += fmt::format("{}", *ptr);
            }
        },
        value);
}

}

void ProcUtils::appendJsonString(std::string& out, std::string_view s) {
    out += '"';
    for (const char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    out += fmt::format("\\u{:04x}", static_cast<unsigned char>(c));
                } else {
                    out += c;
                }
        }
    }
    out += '"';
}

void ProcUtils::encodeProperties(const EntityPropertyView& props,
                                 const PropertyTypeMap& propTypes,
                                 std::string& out) {
    // Collect (name, value) and sort by name so the JSON key order is
    // deterministic regardless of the storage layer's property iteration order,
    // keeping the output reproducible across builds and platforms.
    std::vector<std::pair<std::string_view, PropertyVariant>> namedValues;
    for (const PropertyView& pv : props) {
        const auto name = propTypes.getName(pv._id);
        if (!name) {
            continue;
        }
        namedValues.emplace_back(name.value(), pv._value);
    }
    std::sort(namedValues.begin(), namedValues.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    out.clear();
    out += '{';
    bool first = true;
    for (const auto& [name, value] : namedValues) {
        if (!first) {
            out += ',';
        }
        first = false;
        appendJsonString(out, name);
        out += ':';
        appendPropertyValue(out, value);
    }
    out += '}';
}

void ProcUtils::readIntList(const ListView* view, std::vector<int64_t>& out) {
    out.clear();
    for (const CellView& el : *view) {
        switch (el.getTag()) {
            case CellTag::Int:
                out.push_back(el.getAs<int64_t>());
                break;
            default:
                break;
        }
    }
}
