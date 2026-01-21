#include "JsonImporter.h"

#include <nlohmann/json.hpp>

#include "JsonSaxParser.h"

using namespace vec;

ImportResult<void> JsonImporter::import(BatchVectorCreate& batch,
                                        const std::string& data) {
    if (data.empty()) {
        return ImportError::result(ImportErrorCode::InvalidData, "JSON is empty");
    }

    JsonSaxParser parser(batch);

    try {
        nlohmann::json::sax_parse(data, &parser, nlohmann::json::input_format_t::json, true, true);
    } catch (const ImportException& e) {
        return nonstd::make_unexpected(e.err());
    } catch (const nlohmann::json::exception& e) {
        return ImportError::result(ImportErrorCode::JSONParseError, e.what());
    }

    return {};
}
