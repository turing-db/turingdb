#include "ParquetSchemaExtractor.h"

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "ParquetSchema.h"

using namespace db;

ParquetSchemaExtractor::ParquetSchemaExtractor(ParquetSchema& schema)
    : _schema(schema)
{
}

ParquetSchemaExtractor::~ParquetSchemaExtractor() {
}

void ParquetSchemaExtractor::extractField(const parquet::schema::Node& node,
                                          ParquetSchemaField& out) {
    out.setName(node.name());

    switch (node.repetition()) {
        case parquet::Repetition::REQUIRED:
            out.setRepetition(ParquetFieldRepetition::REQUIRED);
        break;
        case parquet::Repetition::OPTIONAL:
            out.setRepetition(ParquetFieldRepetition::OPTIONAL);
        break;
        case parquet::Repetition::REPEATED:
            out.setRepetition(ParquetFieldRepetition::REPEATED);
        break;
        default:
            out.setRepetition(ParquetFieldRepetition::UNDEFINED);
        break;
    }

    const auto& logicalType = node.logical_type();
    if (logicalType && !logicalType->is_none()) {
        out.setLogicalType(logicalType->ToString());
    }

    if (node.is_group()) {
        out.markAsGroup();
        const auto& group = static_cast<const parquet::schema::GroupNode&>(node);
        const size_t fieldCount = static_cast<size_t>(group.field_count());
        for (size_t fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
            ParquetSchemaField& child = out.addChild();
            extractField(*group.field(static_cast<int>(fieldIndex)), child);
        }
    } else {
        const auto& primitive = static_cast<const parquet::schema::PrimitiveNode&>(node);
        switch (primitive.physical_type()) {
            case parquet::Type::BOOLEAN:
                out.setPrimitiveType(ParquetPrimitiveType::BOOLEAN);
            break;
            case parquet::Type::INT32:
                out.setPrimitiveType(ParquetPrimitiveType::INT32);
            break;
            case parquet::Type::INT64:
                out.setPrimitiveType(ParquetPrimitiveType::INT64);
            break;
            case parquet::Type::INT96:
                out.setPrimitiveType(ParquetPrimitiveType::INT96);
            break;
            case parquet::Type::FLOAT:
                out.setPrimitiveType(ParquetPrimitiveType::FLOAT);
            break;
            case parquet::Type::DOUBLE:
                out.setPrimitiveType(ParquetPrimitiveType::DOUBLE);
            break;
            case parquet::Type::BYTE_ARRAY:
                out.setPrimitiveType(ParquetPrimitiveType::BYTE_ARRAY);
            break;
            case parquet::Type::FIXED_LEN_BYTE_ARRAY:
                out.setPrimitiveType(ParquetPrimitiveType::FIXED_LEN_BYTE_ARRAY);
                out.setFixedLength(static_cast<size_t>(primitive.type_length()));
            break;
            default:
            break;
        }
    }
}

bool ParquetSchemaExtractor::onFileStart(const parquet::FileMetaData& metadata) {
    _schema.setCreatedBy(metadata.created_by());
    _schema.setVersion(parquet::ParquetVersionToString(metadata.version()));
    _schema.setRowCount(static_cast<size_t>(metadata.num_rows()));
    _schema.setRowGroupCount(static_cast<size_t>(metadata.num_row_groups()));
    _schema.setColumnCount(static_cast<size_t>(metadata.num_columns()));

    const parquet::SchemaDescriptor* schemaDescriptor = metadata.schema();
    const parquet::schema::GroupNode* root = schemaDescriptor->group_node();
    extractField(*root, _schema.getRoot());

    // Stop the streaming read after the metadata callback — the schema is all we need.
    return false;
}
