#include <stdlib.h>

#include <exception>
#include <iostream>
#include <string>

#include <argparse.hpp>

#include "ParquetReader.h"
#include "Path.h"

#include "ParquetJsonDetector.h"
#include "ParquetSchema.h"
#include "ParquetSchemaExtractor.h"

#include "TuringException.h"

using namespace db;

namespace {

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

        const std::string jsonSuffix = field.isLikelyJson() ? " [likely JSON]" : "";

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

void runSchemaCommand(const fs::Path& path) {
    ParquetSchema schema;

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

    printSchema(schema);
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

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    if (!printSchemaFlag) {
        std::cerr << "No action specified.\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    try {
        const fs::Path path(filePath);

        if (printSchemaFlag) {
            runSchemaCommand(path);
        }
    } catch (const TuringException& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
