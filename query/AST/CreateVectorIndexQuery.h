#pragma once

#include "QueryCommand.h"

#include <stdint.h>
#include <string_view>

#include "VecLibMetadata.h"

namespace db {

class CypherAST;
class DeclContext;

class CreateVectorIndexQuery : public QueryCommand {
public:
    static CreateVectorIndexQuery* create(CypherAST* ast,
                                          std::string_view indexName,
                                          vec::Dimension dimension,
                                          vec::DistanceMetric metric,
                                          vec::IndexType indexType);

    Kind getKind() const override { return Kind::CREATE_VECTOR_INDEX_QUERY; }

    std::string_view getIndexName() const { return _indexName; }
    vec::Dimension getDimension() const { return _dimension; }
    vec::DistanceMetric getMetric() const { return _metric; }
    vec::IndexType getIndexType() const { return _indexType; }

private:
    std::string_view _indexName;
    vec::Dimension _dimension {0};
    vec::DistanceMetric _metric {vec::DistanceMetric::EUCLIDEAN_DIST};
    vec::IndexType _indexType {vec::IndexType::FLAT};

    CreateVectorIndexQuery(DeclContext* declContext,
                           std::string_view indexName,
                           vec::Dimension dimension,
                           vec::DistanceMetric metric,
                           vec::IndexType indexType);
    ~CreateVectorIndexQuery() override;
};

}
