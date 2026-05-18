#include <stdlib.h>

#include <algorithm>
#include <array>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <argparse.hpp>
#include <spdlog/fmt/fmt.h>
#include <tabulate/table.hpp>

#include "ParquetReader.h"
#include "Path.h"

#include "ParquetGraphMapping.h"
#include "ParquetJsonDetector.h"
#include "ParquetPropertyAnalysis.h"
#include "ParquetPropertyAnalyzer.h"
#include "ParquetPropertyMerge.h"
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

void processFile(const std::string& filePath,
                 const ParquetSchema& schema,
                 const std::vector<std::string>& propsColumns,
                 const std::string& mappingColumnName) {
    const fs::Path path(filePath);

    printSchema(schema);

    for (const std::string& propsColumn : propsColumns) {
        std::cout << "\n";
        runPropertyAnalysis(path, schema, propsColumn);
    }

    if (!mappingColumnName.empty()) {
        std::cout << "\n";
        runGraphMapping(path, schema, mappingColumnName);
    }
}

bool fileHasTopLevelColumn(const ParquetSchema& schema, const std::string& name) {
    const ParquetSchemaField& root = schema.getRoot();
    const size_t childCount = root.getChildCount();
    for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
        if (root.getChild(childIndex).getName() == name) {
            return true;
        }
    }
    return false;
}

std::string trimWhitespace(const std::string& value) {
    const size_t start = value.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) {
        return "";
    }
    const size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(start, end - start + 1);
}

void runMergedPropertyPrompt(const std::vector<std::string>& files,
                             const std::vector<std::unique_ptr<ParquetSchema>>& schemas) {
    std::string defaultColumn = "properties";
    for (const auto& schema : schemas) {
        if (!fileHasTopLevelColumn(*schema, defaultColumn)) {
            defaultColumn.clear();
            break;
        }
    }

    std::cout << "\n";
    if (!defaultColumn.empty()) {
        std::cout << "Column to merge property analysis on [" << defaultColumn << "] (empty to skip): ";
    } else {
        std::cout << "Column to merge property analysis on (empty to skip): ";
    }
    std::cout.flush();

    std::string line;
    if (!std::getline(std::cin, line)) {
        return;
    }

    const std::string trimmed = trimWhitespace(line);
    const std::string columnName = trimmed.empty() ? defaultColumn : trimmed;
    if (columnName.empty()) {
        return;
    }

    ParquetPropertyAnalysis merged;
    ParquetPropertyMerge merger(merged);
    for (size_t fileIndex = 0; fileIndex < files.size(); ++fileIndex) {
        const fs::Path path(files[fileIndex]);
        ParquetPropertyAnalysis analysis;
        ParquetPropertyAnalyzer analyzer(*schemas[fileIndex], columnName, analysis);
        ParquetReader reader(path, analyzer);
        while (reader.nextChunk()) {
        }
        merger.merge(analysis);
    }

    std::cout << "\n== Merged property analysis ==\n";
    printPropertyAnalysis(merged, columnName);
}

}

int main(int argc, const char** argv) {
    argparse::ArgumentParser parser("turing-parquet", "1.0", argparse::default_arguments::help);
    parser.add_description("TuringDB - Parquet inspection tool");

    std::string filePath;
    std::vector<std::string> nodeFiles;
    std::vector<std::string> edgeFiles;
    std::unordered_map<std::string, std::vector<std::string>> propsByFile;
    std::vector<std::string> pendingProps;
    std::string currentFilePath;

    const auto attachPendingProps = [&](const std::string& file) {
        for (const std::string& column : pendingProps) {
            propsByFile[file].push_back(column);
        }
        pendingProps.clear();
    };

    parser.add_argument("file")
        .metavar("FILE")
        .help("Parquet file to inspect (optional if -nodes or -edges is given)")
        .nargs(argparse::nargs_pattern::optional)
        .action([&](const std::string& value) {
            filePath = value;
            currentFilePath = value;
            attachPendingProps(value);
        });

    parser.add_argument("-nodes")
        .metavar("FILE")
        .help("Parquet file to inspect as a node table (repeatable)")
        .append()
        .action([&](const std::string& value) {
            nodeFiles.push_back(value);
            currentFilePath = value;
            attachPendingProps(value);
        });

    parser.add_argument("-edges")
        .metavar("FILE")
        .help("Parquet file to inspect as an edge table (repeatable)")
        .append()
        .action([&](const std::string& value) {
            edgeFiles.push_back(value);
            currentFilePath = value;
            attachPendingProps(value);
        });

    parser.add_argument("-props")
        .metavar("COLUMN")
        .help("Analyze the JSON property structure of COLUMN on the most recent file (repeatable)")
        .append()
        .action([&](const std::string& value) {
            if (currentFilePath.empty()) {
                pendingProps.push_back(value);
            } else {
                propsByFile[currentFilePath].push_back(value);
            }
        });

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

    if (!pendingProps.empty()) {
        std::cerr << "-props specified without a preceding file.\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    std::vector<std::string> allFiles;
    allFiles.reserve(nodeFiles.size() + edgeFiles.size() + 1);
    allFiles.insert(allFiles.end(), nodeFiles.begin(), nodeFiles.end());
    allFiles.insert(allFiles.end(), edgeFiles.begin(), edgeFiles.end());
    if (!filePath.empty()) {
        allFiles.push_back(filePath);
    }

    if (allFiles.empty()) {
        std::cerr << "No input files specified.\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    try {
        std::vector<std::unique_ptr<ParquetSchema>> schemas;
        schemas.reserve(allFiles.size());
        for (const std::string& file : allFiles) {
            schemas.push_back(std::make_unique<ParquetSchema>());
            const fs::Path path(file);
            buildSchema(path, *schemas.back());
        }

        for (size_t fileIndex = 0; fileIndex < allFiles.size(); ++fileIndex) {
            if (fileIndex > 0) {
                std::cout << "\n";
            }
            std::cout << "== " << allFiles[fileIndex] << " ==\n";

            const auto propsIter = propsByFile.find(allFiles[fileIndex]);
            const std::vector<std::string> emptyProps;
            const std::vector<std::string>& fileProps = (propsIter != propsByFile.end()) ? propsIter->second : emptyProps;
            processFile(allFiles[fileIndex], *schemas[fileIndex], fileProps, mappingColumnName);
        }

        runMergedPropertyPrompt(allFiles, schemas);
    } catch (const TuringException& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
