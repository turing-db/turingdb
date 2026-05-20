#include <stdlib.h>

#include <algorithm>
#include <array>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <argparse.hpp>
#include <spdlog/fmt/fmt.h>
#include <tabulate/table.hpp>

#include "Graph.h"
#include "JobSystem.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"

#include "ParquetReader.h"

#include "ParquetEdgeTypeAnalysis.h"
#include "ParquetEdgeTypeAnalyzer.h"
#include "ParquetGraphImporter.h"
#include "ParquetGraphMapping.h"
#include "ParquetJsonDetector.h"
#include "ParquetPropertyAnalysis.h"
#include "ParquetPropertyAnalyzer.h"
#include "ParquetPropertyMerge.h"
#include "ParquetSchema.h"
#include "ParquetSchemaExtractor.h"

#include "Path.h"
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
        fmt::println("  {:<8} {:>8}  ({:5.1f}%)",
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
            fmt::println("  {:<{}}  {:<{}}  ({})",
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

void runMergedPropertyAnalysis(const std::vector<std::string>& files,
                               const std::vector<std::unique_ptr<ParquetSchema>>& schemas,
                               const std::string& columnName,
                               ParquetGraphMapping& outMapping) {
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

    std::cout << "\n== Merged property analysis: " << columnName << " ==\n";
    printPropertyAnalysis(merged, columnName);

    ParquetGraphMapping::buildFrom(merged, columnName, outMapping);
    ParquetGraphMapping::inferLabelNames(outMapping);

    std::cout << "\n== Graph mapping: " << columnName << " ==\n";
    printGraphMapping(outMapping);
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
                             const std::vector<std::unique_ptr<ParquetSchema>>& schemas,
                             std::string& outColumnName,
                             ParquetGraphMapping& outMapping) {
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
    const bool hasInput = static_cast<bool>(std::getline(std::cin, line));
    const std::string trimmed = hasInput ? trimWhitespace(line) : "";
    const std::string columnName = trimmed.empty() ? defaultColumn : trimmed;
    if (columnName.empty()) {
        return;
    }
    if (!hasInput) {
        std::cout << defaultColumn << " (stdin closed; using default)\n";
    }

    outColumnName = columnName;
    runMergedPropertyAnalysis(files, schemas, columnName, outMapping);
}

void printEdgeTypeAnalysis(const ParquetEdgeTypeAnalysis& analysis,
                           const std::string& columnName) {
    std::cout << "Edge type analysis on column '" << columnName << "':\n";
    std::cout << "Total values: " << analysis.getTotalCount() << "\n\n";

    const ParquetEdgeTypeAnalysis::TypeCountMap& typeCounts = analysis.getTypeCounts();
    if (typeCounts.empty()) {
        std::cout << "(no values)\n";
        return;
    }

    std::vector<std::pair<std::string, size_t>> sorted(typeCounts.begin(), typeCounts.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) {
                  return a.second > b.second;
              });

    size_t maxNameWidth = 0;
    for (const auto& entry : sorted) {
        maxNameWidth = std::max(maxNameWidth, entry.first.size());
    }

    const size_t total = analysis.getTotalCount();
    std::cout << "Type breakdown:\n";
    for (const auto& entry : sorted) {
        const double percent = (total == 0) ? 0.0 : 100.0 * static_cast<double>(entry.second) / static_cast<double>(total);
        fmt::println("  {:<{}}  {:>8}  ({:5.1f}%)",
                     entry.first,
                     maxNameWidth,
                     entry.second,
                     percent);
    }
}

void runEdgeTypeAnalysis(const std::vector<std::string>& edgeFiles,
                         const std::vector<const ParquetSchema*>& edgeSchemas,
                         const std::string& columnName) {
    ParquetEdgeTypeAnalysis analysis;
    for (size_t fileIndex = 0; fileIndex < edgeFiles.size(); ++fileIndex) {
        const fs::Path path(edgeFiles[fileIndex]);
        ParquetEdgeTypeAnalyzer analyzer(*edgeSchemas[fileIndex], columnName, analysis);
        ParquetReader reader(path, analyzer);
        while (reader.nextChunk()) {
        }
    }

    std::cout << "\n== Edge type analysis: " << columnName << " ==\n";
    printEdgeTypeAnalysis(analysis, columnName);
}

void runEdgeTypePrompt(const std::vector<std::string>& edgeFiles,
                       const std::vector<const ParquetSchema*>& edgeSchemas) {
    if (edgeFiles.empty()) {
        return;
    }

    std::string defaultColumn;
    for (const ParquetSchema* schema : edgeSchemas) {
        if (fileHasTopLevelColumn(*schema, "relation")) {
            defaultColumn = "relation";
            break;
        }
    }

    std::cout << "\n";
    if (!defaultColumn.empty()) {
        std::cout << "Column to analyze edge types on [" << defaultColumn << "] (empty to skip): ";
    } else {
        std::cout << "Column to analyze edge types on (empty to skip): ";
    }
    std::cout.flush();

    std::string line;
    const bool hasInput = static_cast<bool>(std::getline(std::cin, line));
    const std::string trimmed = hasInput ? trimWhitespace(line) : "";
    const std::string columnName = trimmed.empty() ? defaultColumn : trimmed;
    if (columnName.empty()) {
        return;
    }
    if (!hasInput) {
        std::cout << defaultColumn << " (stdin closed; using default)\n";
    }

    runEdgeTypeAnalysis(edgeFiles, edgeSchemas, columnName);
}

}

int main(int argc, const char** argv) {
    argparse::ArgumentParser parser("turing-parquet", "1.0", argparse::default_arguments::help);
    parser.add_description("TuringDB - Parquet inspection tool");

    std::vector<std::string> nodeFiles;
    std::vector<std::string> edgeFiles;
    std::vector<std::string> propsColumns;
    std::string edgeTypeColumn;
    std::string outputDir = "turingdb.out";
    std::string graphName = "imported";

    parser.add_argument("-nodes")
        .metavar("FILE")
        .help("Parquet file to inspect as a node table (repeatable)")
        .append()
        .store_into(nodeFiles);

    parser.add_argument("-edges")
        .metavar("FILE")
        .help("Parquet file to inspect as an edge table (repeatable)")
        .append()
        .store_into(edgeFiles);

    parser.add_argument("-props")
        .metavar("COLUMN")
        .help("Run merged JSON property analysis on COLUMN across all input files (repeatable). "
              "If omitted, you will be prompted for a column.")
        .append()
        .store_into(propsColumns);

    parser.add_argument("-edgetype")
        .metavar("COLUMN")
        .help("Run edge-type analysis on COLUMN across all edge files. "
              "If omitted, you will be prompted for a column.")
        .store_into(edgeTypeColumn);

    parser.add_argument("-out")
        .metavar("DIR")
        .help("TuringDB root directory (default: ./turingdb.out). Created if "
              "absent; otherwise its existing contents (other graphs, system "
              "files) are preserved. Only the named graph's subdirectory is "
              "wiped before the import.")
        .store_into(outputDir);

    parser.add_argument("-graph")
        .metavar("NAME")
        .help("Graph name to create inside -out DIR (default: imported). If "
              "a graph with this name already exists in the directory, its "
              "subdirectory is removed before writing the new one.")
        .store_into(graphName);

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    std::vector<std::string> allFiles;
    allFiles.reserve(nodeFiles.size() + edgeFiles.size());
    allFiles.insert(allFiles.end(), nodeFiles.begin(), nodeFiles.end());
    allFiles.insert(allFiles.end(), edgeFiles.begin(), edgeFiles.end());

    if (allFiles.empty()) {
        std::cerr << "No input files specified.\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    try {
        // Build schema for each file
        std::vector<std::unique_ptr<ParquetSchema>> schemas;
        schemas.reserve(allFiles.size());
        for (const std::string& file : allFiles) {
            schemas.push_back(std::make_unique<ParquetSchema>());
            const fs::Path path(file);
            buildSchema(path, *schemas.back());
        }

        // Print each schema
        for (size_t fileIndex = 0; fileIndex < allFiles.size(); ++fileIndex) {
            if (fileIndex > 0) {
                std::cout << "\n";
            }
            std::cout << "== " << allFiles[fileIndex] << " ==\n";
            printSchema(*schemas[fileIndex]);
        }

        // Do property column analysis
        std::unique_ptr<ParquetGraphMapping> propertyMapping;
        std::string propertyColumn;
        if (!propsColumns.empty()) {
            for (const std::string& column : propsColumns) {
                propertyMapping = std::make_unique<ParquetGraphMapping>();
                runMergedPropertyAnalysis(allFiles, schemas, column, *propertyMapping);
                propertyColumn = column;
            }
        } else {
            propertyMapping = std::make_unique<ParquetGraphMapping>();
            runMergedPropertyPrompt(allFiles, schemas, propertyColumn, *propertyMapping);
            if (propertyColumn.empty()) {
                propertyMapping.reset();
            }
        }

        // Infer edge types
        std::vector<const ParquetSchema*> edgeSchemas;
        edgeSchemas.reserve(edgeFiles.size());
        for (size_t edgeIndex = 0; edgeIndex < edgeFiles.size(); ++edgeIndex) {
            edgeSchemas.push_back(schemas[nodeFiles.size() + edgeIndex].get());
        }

        std::string resolvedEdgeTypeColumn = edgeTypeColumn;
        if (!edgeTypeColumn.empty()) {
            if (!edgeFiles.empty()) {
                runEdgeTypeAnalysis(edgeFiles, edgeSchemas, edgeTypeColumn);
            }
        } else {
            runEdgeTypePrompt(edgeFiles, edgeSchemas);
            if (resolvedEdgeTypeColumn.empty()) {
                for (const ParquetSchema* schema : edgeSchemas) {
                    if (fileHasTopLevelColumn(*schema, "relation")) {
                        resolvedEdgeTypeColumn = "relation";
                        break;
                    }
                }
            }
        }

        // Check that we have a property mapping and that edge types are resolved
        if (propertyMapping == nullptr) {
            std::cerr << "Cannot import without a resolved property column "
                         "(use -props COLUMN or accept the prompt default).\n";
            return EXIT_FAILURE;
        } else if (!edgeFiles.empty() && resolvedEdgeTypeColumn.empty()) {
            std::cerr << "Cannot import edges without a resolved edge-type "
                         "column (use -edgetype COLUMN).\n";
            return EXIT_FAILURE;
        }

        // Init TuringDB in-process
        fs::Path turingDir(outputDir);
        if (!turingDir.toAbsolute()) {
            std::cerr << "Failed to resolve -out path '" << outputDir << "'.\n";
            return EXIT_FAILURE;
        }

        fs::Path graphSubDir = turingDir / "graphs" / graphName;
        if (graphSubDir.exists()) {
            std::cout << "Removing existing graph subdirectory " << graphSubDir.c_str() << "\n";
            graphSubDir.rm();
        }

        TuringConfig config;
        config.setTuringDirectory(turingDir);

        std::cout << "\n== Import into " << turingDir.c_str()
                  << " (graph '" << graphName << "') ==\n";

        TuringDB db(&config);
        db.init();
        Graph* const graph = db.getSystemManager().createGraph(graphName);

        JobSystem jobSystem;
        jobSystem.init();

        // Import the graph
        ParquetGraphImporter importer(graph,
                                      &jobSystem,
                                      *propertyMapping,
                                      propertyColumn,
                                      resolvedEdgeTypeColumn);

        for (size_t nodeIndex = 0; nodeIndex < nodeFiles.size(); ++nodeIndex) {
            const fs::Path path(nodeFiles[nodeIndex]);
            const ParquetSchema* const schema = schemas[nodeIndex].get();
            importer.importNodeFile(path, *schema);
        }
        for (size_t edgeIndex = 0; edgeIndex < edgeFiles.size(); ++edgeIndex) {
            const fs::Path path(edgeFiles[edgeIndex]);
            const ParquetSchema* const schema = schemas[nodeFiles.size() + edgeIndex].get();
            importer.importEdgeFile(path, *schema);
        }
        importer.finalize();

        // Dump the graph
        const auto dumpResult = db.getSystemManager().dumpGraph(graphName);
        if (!dumpResult) {
            std::cerr << "Failed to persist graph: "
                      << dumpResult.error().fmtMessage() << "\n";
            return EXIT_FAILURE;
        }

        fmt::println(
            "Imported {} nodes, {} sub-record nodes ({} duplicate references "
            "reused), {} stub nodes (unresolved edge endpoints), {} edges "
            "({} skipped).",
            importer.getNodeCount(),
            importer.getSubRecordCount(),
            importer.getDedupedReferenceCount(),
            importer.getStubNodeCount(),
            importer.getEdgeCount(),
            importer.getSkippedEdgeCount());
        std::cout << "Wrote graph '" << graphName << "' at " << turingDir.c_str() << "\n";
    } catch (const TuringException& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
