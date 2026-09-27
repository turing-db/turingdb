#include "WritesRollback.h"

#include "CypherAST.h"
#include "QueryCommand.h"

#include "versioning/CommitWriteBuffer.h"
#include "writers/MetadataBuilder.h"

using namespace db;

WritesRollback::WritesRollback(CommitWriteBuffer* writeBuffer, MetadataBuilder* metadataBuilder)
    : _writeBuffer(writeBuffer),
    _metadataBuilder(metadataBuilder)
{
    if (_writeBuffer) {
        _writeBuffer->beginStatement();
    }

    if (_metadataBuilder) {
        _metadataBuilder->beginStatement();
    }
}

WritesRollback::~WritesRollback() {
    if (_writeBuffer) {
        _writeBuffer->rollbackStatement();
    }

    if (_metadataBuilder) {
        _metadataBuilder->rollbackStatement();
    }
}

bool WritesRollback::isEnabledFor(const CypherAST& ast) {
    for (const QueryCommand* query : ast.queries()) {
        const QueryCommand::Kind kind = query->getKind();
        const bool isQuery = kind == QueryCommand::Kind::SINGLE_PART_QUERY
                          || kind == QueryCommand::Kind::UNION_QUERY;

        if (!isQuery) {
            return false;
        }
    }

    return true;
}

void WritesRollback::keep() {
    if (_writeBuffer) {
        _writeBuffer->endStatement();
    }

    _writeBuffer = nullptr;
    _metadataBuilder = nullptr;
}
