#include <stdlib.h>

#include <algorithm>
#include <array>
#include <exception>
#include <iostream>
#include <string>

#include <argparse.hpp>
#include <spdlog/fmt/fmt.h>
#include <tabulate/table.hpp>

#include "ParquetReader.h"
#include "Path.h"

#include "ParquetGraphMapping.h"
#include "ParquetJsonDetector.h"
#include "ParquetPropertyAnalysis.h"
#include "ParquetPropertyAnalyzer.h"
#include "ParquetSchema.h"
#include "ParquetSchemaExtractor.h"

#include "TuringException.h"

using namespace db;

namespace {

std::string propertyTypeLabel(const ParquetPropertyType& propertyType) {
    if (propertyType.isMixed()) {
        return "mixed";
    }

    const ParquetJsonValueType valueType = propertyType.getValueType();
    const std::string typeName = ParquetPropertyAnalysis::toString(valueType);
    if (propertyType.isNullable() && valueType != ParquetJsonValueType::NIL) {
        return "nullable " + typeName;
    }
    return typeName;
}

void printField(const ParquetSchemaField& field, size_t depth) {
    const std::string indent(depth * 2, ' ');
    const char* repetition = ParquetSchema::toString(field.getRepetition());

    const std::string& logicalType = field.getLogicalType();
    const std::string logicalSuffix = logicalType.empty() ? "" : " (" + logicalType + ")";

    if (field.isGroup()) {
        std::cout << indent << repetition << " group "
                  << field.getName() << logicalSuffix << " {\n";

        const size_t childCount = field.getChildCount();
        for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
            printField(field.getChild(childIndex), depth + 1);
        }

        std::cout << indent << "}\n";
    } else {
        const ParquetPrimitiveType primitiveType = field.getPrimitiveType();
        std::string typeString = ParquetSchema::toString(primitiveType);
        if (primitiveType == ParquetPrimitiveType::FIXED_LEN_BYTE_ARRAY) {
            typeString += "(" + std::to_string(field.getFixedLength()) + ")";
        }

        std::string jsonSuffix;
        if (field.getJsonShape() == ParquetJsonShape::KEY_VALUE) {
            jsonSuffix = " [likely key-value JSON]";
        } else if (field.getJsonShape() == ParquetJsonShape::GENERAL) {
            jsonSuffix = " [likely JSON]";
        }

        std::cout << indent << repetition << " " << typeString << " "
                  << field.getName() << logicalSuffix << jsonSuffix << "\n";
    }
}

void printSchema(const ParquetSchema& schema) {
    std::cout << "Created by: " << schema.getCreatedBy() << "\n";
    std::cout << "Version:    " << schema.getVersion() << "\n";
    std::cout << "Rows:       " << schema.getRowCount() << "\n";
    std::cout << "Row groups: " << schema.getRowGroupCount() << "\n";
    std::cout << "Columns:    " << schema.getColumnCount() << "\n";
    std::cout << "\n";
    std::cout << "Schema:\n";

    const ParquetSchemaField& root = schema.getRoot();
    const size_t childCount = root.getChildCount();
    for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
        printField(root.getChild(childIndex), 0);
    }
}

void printPropertyAnalysis(const ParquetPropertyAnalysis& analysis,
                           const std::string& columnName) {
    std::cout << "Property analysis for column '" << columnName << "':\n";
    std::cout << "Total values: " << analysis.getTotalCount() << "\n\n";

    std::array<ParquetJsonValueType, ParquetPropertyAnalysis::TYPE_COUNT> types {
        ParquetJsonValueType::NIL,
        ParquetJsonValueType::BOOLEAN,
        ParquetJsonValueType::INTEGER,
        ParquetJsonValueType::FLOAT,
        ParquetJsonValueType::STRING,
        ParquetJsonValueType::ARRAY,
        ParquetJsonValueType::OBJECT,
    };
    std::sort(types.begin(), types.end(),
              [&](ParquetJsonValueType a, ParquetJsonValueType b) {
                  return analysis.getTypeCount(a) > analysis.getTypeCount(b);
              });

    std::cout << "Type breakdown:\n";
    const size_t total = analysis.getTotalCount();
    for (const ParquetJsonValueType type : types) {
        const size_t count = analysis.getTypeCount(type);
        if (count == 0) {
            continue;
        }
        const double percent = (total == 0) ? 0.0 : 100.0 * static_cast<double>(count) / static_cast<double>(total);
        std::cout << fmt::format("  {:<8} {:>8}  ({:5.1f}%)\n",
                                 ParquetPropertyAnalysis::toString(type),
                                 count,
                                 percent);
    }

    const ParquetPropertyAnalysis::PropertyTypeMap& propertyTypes = analysis.getPropertyTypes();
    if (!propertyTypes.empty()) {
        std::cout << "\nProperty types (" << propertyTypes.size() << " unique):\n";

        size_t maxNameWidth = 0;
        size_t maxTypeWidth = 0;
        for (const auto& entry : propertyTypes) {
            maxNameWidth = std::max(maxNameWidth, entry.first.size());
            maxTypeWidth = std::max(maxTypeWidth, propertyTypeLabel(*entry.second).size());
        }

        for (const auto& entry : propertyTypes) {
            const ParquetPropertyType& propertyType = *entry.second;
            std::cout << fmt::format("  {:<{}}  {:<{}}  ({})\n",
                                     propertyType.getName(),
                                     maxNameWidth,
                                     propertyTypeLabel(propertyType),
                                     maxTypeWidth,
                                     propertyType.getCount());
        }
    }

    const std::vector<std::string>& arrayPreviews = analysis.getArrayPreviews();
    if (!arrayPreviews.empty()) {
        std::cout << "\nArray previews:\n";
        for (const std::string& preview : arrayPreviews) {
            std::cout << "  " << preview << "\n";
        }
    }

    const std::vector<std::string>& objectPreviews = analysis.getObjectPreviews();
    if (!objectPreviews.empty()) {
        std::cout << "\nObject previews:\n";
        for (const std::string& preview : objectPreviews) {
            std::cout << "  " << preview << "\n";
        }
    }
}

void buildSchema(const fs::Path& path, ParquetSchema& schema) {
    {
        ParquetSchemaExtractor extractor(schema);
        ParquetReader reader(path, extractor);
        reader.nextChunk();
    }

    {
        ParquetJsonDetector detector(schema);
        ParquetReader reader(path, detector);
        while (reader.nextChunk()) {
        }
    }
}

void runPropertyAnalysis(const fs::Path& path,
                         const ParquetSchema& schema,
                         const std::string& columnName) {
    ParquetPropertyAnalysis analysis;
    ParquetPropertyAnalyzer analyzer(schema, columnName, analysis);

    ParquetReader reader(path, analyzer);
    while (reader.nextChunk()) {
    }

    printPropertyAnalysis(analysis, columnName);
}

std::string propertyTypeString(const ParquetGraphProperty& property) {
    std::string typeString = ParquetGraphMapping::toString(property.getType());
    if (property.isNullable()) {
        typeString += "?";
    }
    if (property.isRawJson()) {
        typeString += " (raw JSON)";
    }
    return typeString;
}

void renderPropertiesTable(const ParquetGraphLabel& label) {
    tabulate::Table table;
    table.add_row({"name", "type"});
    for (const auto& property : label.getProperties()) {
        table.add_row({property->getName(), propertyTypeString(*property)});
    }

    const size_t rowCount = label.getProperties().size() + 1;
    for (size_t rowIndex = 2; rowIndex < rowCount; ++rowIndex) {
        table[rowIndex].format().hide_border_top();
    }
    table[0].format().font_style({tabulate::FontStyle::bold});
    table.column(0).format().font_align(tabulate::FontAlign::left);
    table.column(1).format().font_align(tabulate::FontAlign::left);

    std::cout << table << "\n";
}

std::string labelHeader(const ParquetGraphLabel& label, const std::string& path) {
    std::string header = path;
    if (!label.getInferredLabel().empty()) {
        header += "  :";
        header += label.getInferredLabel();
    }
    header += "  ";
    header += ParquetGraphMapping::toString(label.getCardinality());
    if (label.isNullable()) {
        header += " (nullable)";
    }
    return header;
}

void renderLabelTables(const ParquetGraphLabel& label, const std::string& path) {
    std::cout << "\n" << labelHeader(label, path) << "\n";
    if (label.getProperties().empty()) {
        std::cout << "(no properties)\n";
    } else {
        renderPropertiesTable(label);
    }

    const std::string separator = (label.getCardinality() == ParquetEdgeCardinality::MANY)
                                  ? "[]."
                                  : ".";
    for (const auto& subLabel : label.getSubLabels()) {
        const std::string subPath = path + separator + subLabel->getName();
        renderLabelTables(*subLabel, subPath);
    }
}

void printGraphMapping(const ParquetGraphMapping& mapping) {
    std::cout << "Graph mapping for column '" << mapping.getColumnName() << "':\n";

    const ParquetGraphLabel& root = mapping.getRoot();
    if (!root.getProperties().empty()) {
        std::cout << "\nProperties:\n";
        renderPropertiesTable(root);
    }

    if (!root.getSubLabels().empty()) {
        std::cout << "\nSub-records:\n";
        for (const auto& subLabel : root.getSubLabels()) {
            renderLabelTables(*subLabel, subLabel->getName());
        }
    }

    const std::vector<std::string>& warnings = mapping.getWarnings();
    if (!warnings.empty()) {
        std::cout << "\nWarnings (" << warnings.size() << "):\n";
        for (const std::string& warning : warnings) {
            std::cout << "  - " << warning << "\n";
        }
    }
}

void runGraphMapping(const fs::Path& path,
                     const ParquetSchema& schema,
                     const std::string& columnName) {
    ParquetPropertyAnalysis analysis;
    ParquetPropertyAnalyzer analyzer(schema, columnName, analysis);

    ParquetReader reader(path, analyzer);
    while (reader.nextChunk()) {
    }

    ParquetGraphMapping mapping;
    ParquetGraphMapping::buildFrom(analysis, columnName, mapping);
    ParquetGraphMapping::inferLabelNames(mapping);

    printGraphMapping(mapping);
}

}

int main(int argc, const char** argv) {
    argparse::ArgumentParser parser("turing-parquet", "1.0", argparse::default_arguments::help);
    parser.add_description("TuringDB - Parquet inspection tool");

    std::string filePath;
    parser.add_argument("file")
        .metavar("FILE")
        .help("Parquet file to inspect")
        .store_into(filePath);

    bool printSchemaFlag = false;
    parser.add_argument("-schema")
        .help("Pretty-print the schema of FILE")
        .store_into(printSchemaFlag);

    std::string propsColumnName;
    parser.add_argument("-props")
        .metavar("COLUMN")
        .help("Analyze the JSON property structure of COLUMN (must be key-value JSON)")
        .store_into(propsColumnName);

    std::string mappingColumnName;
    parser.add_argument("-mapping")
        .metavar("COLUMN")
        .help("Derive a TuringDB graph mapping from the JSON property structure of COLUMN")
        .store_into(mappingColumnName);

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    const bool wantsSchema = printSchemaFlag;
    const bool wantsProps = !propsColumnName.empty();
    const bool wantsMapping = !mappingColumnName.empty();
    if (!wantsSchema && !wantsProps && !wantsMapping) {
        std::cerr << "No action specified.\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    try {
        const fs::Path path(filePath);

        ParquetSchema schema;
        buildSchema(path, schema);

        if (wantsSchema) {
            printSchema(schema);
        }

        if (wantsProps) {
            if (wantsSchema) {
                std::cout << "\n";
            }
            runPropertyAnalysis(path, schema, propsColumnName);
        }

        if (wantsMapping) {
            if (wantsSchema || wantsProps) {
                std::cout << "\n";
            }
            runGraphMapping(path, schema, mappingColumnName);
        }
    } catch (const TuringException& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
