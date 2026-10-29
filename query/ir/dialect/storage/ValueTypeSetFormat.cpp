#include "ValueTypeSetFormat.h"

#include <optional>

#include "metadata/PropertyType.h"
#include "metadata/ValueTypeSet.h"

mlir::FailureOr<uint32_t> mlir::storage::parseValueTypeSet(AsmParser& parser) {
    uint32_t bits = 0;

    const auto parseMember = [&]() -> ParseResult {
        const llvm::SMLoc location = parser.getCurrentLocation();

        StringRef name;
        if (parser.parseKeyword(&name)) {
            return failure();
        }

        const std::optional<::db::ValueType> type = ::db::ValueTypeName::fromString(name);
        const bool namesAValueType = type && *type != ::db::ValueType::Invalid;
        if (!namesAValueType) {
            return parser.emitError(location) << "unknown value type " << name;
        }

        bits |= ::db::ValueTypeSet::of(*type).getBits();
        return success();
    };

    if (parser.parseCommaSeparatedList(AsmParser::Delimiter::Square, parseMember)) {
        return failure();
    }

    return bits;
}

void mlir::storage::printValueTypeSet(AsmPrinter& printer, uint32_t bits) {
    const ::db::ValueTypeSet set = ::db::ValueTypeSet::fromBits(bits);

    printer << '[';

    bool first = true;
    for (uint8_t ordinal = 1; ordinal < static_cast<uint8_t>(::db::ValueType::_SIZE); ordinal++) {
        const ::db::ValueType type = static_cast<::db::ValueType>(ordinal);
        if (!set.contains(type)) {
            continue;
        }

        if (!first) {
            printer << ", ";
        }

        printer << StringRef(::db::ValueTypeName::value(type));
        first = false;
    }

    printer << ']';
}
