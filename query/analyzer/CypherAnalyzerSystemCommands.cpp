#include "CypherAnalyzer.h"

#include <utility>
#include <string_view>

#include "EdgePattern.h"
#include "EntityPattern.h"
#include "Literal.h"
#include "NodePattern.h"
#include "ExprAnalyzer.h"
#include "LoadGraphQuery.h"
#include "CreateGraphQuery.h"
#include "LoadGMLQuery.h"
#include "LoadParquetQuery.h"
#include "LoadJsonlQuery.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "CreateVectorIndexQuery.h"
#include "LoadVectorQuery.h"
#include "LoadEmbeddingQuery.h"
#include "InstallExtensionQuery.h"
#include "expr/PropertyExpr.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateEdgePropertyIndexQuery.h"

#include "BioAssert.h"
#include "FileName.h"

using namespace db;

namespace {

std::string_view importedTypeName(ValueType type) {
    switch (type) {
        case ValueType::DateTime:
            return "a datetime";
        break;
        case ValueType::Duration:
            return "a duration";
        break;
        case ValueType::Point:
            return "a point";
        break;
        default:
            return ValueTypeName::value(type);
        break;
    }
}

}

void CypherAnalyzer::analyze(const LoadGraphQuery* loadGraph) {
    const std::string_view graphName = loadGraph->getGraphName();
    if (graphName.empty()) {
        throwError("LOAD GRAPH should not have an empty graph name");
    }

    throwIfInvalidGraphName(graphName, loadGraph);
}

void CypherAnalyzer::analyze(const CreateGraphQuery* createGraph) {
    const std::string_view graphName = createGraph->getGraphName();
    if (graphName.empty()) {
        throwError("CREATE GRAPH should not have an empty graph name");
    }

    throwIfInvalidGraphName(graphName, createGraph);
}

void CypherAnalyzer::analyze(LoadJsonlQuery* loadJsonl) {
    std::string_view graphName = loadJsonl->getGraphName();
    if (graphName.empty()) {
        graphName = loadJsonl->getFilePath().basename();
    }

    loadJsonl->setGraphName(graphName);

    throwIfInvalidGraphName(graphName, loadJsonl);

    const auto& embDims = loadJsonl->getEmbeddingSpecs();
    for (const auto& [name, dimension] : embDims) {
        if (dimension == 0) {
            throwError(fmt::format("Embedding property '{}' must have dimension greater than 0", name), loadJsonl);
        }
    }

    const TypedPropertySpec& typedProperties = loadJsonl->getTypedPropertySpec();

    for (const TypedPropertySpec::Clash& clash : typedProperties.getClashes()) {
        throwError(fmt::format("Property '{}' is named both {} and {}.",
                               clash._name,
                               importedTypeName(clash._first),
                               importedTypeName(clash._second)),
                   loadJsonl);
    }

    for (const auto& [name, type] : typedProperties) {
        if (embDims.find(name) != embDims.end()) {
            throwError(fmt::format("Property '{}' is named both an embedding and {}.", name, importedTypeName(type)),
                       loadJsonl);
        }
    }
}

void CypherAnalyzer::analyze(LoadGMLQuery* loadGML) {
    std::string_view graphName = loadGML->getGraphName();
    if (graphName.empty()) {
        graphName = loadGML->getFilePath().basename();
    }

    loadGML->setGraphName(graphName);

    throwIfInvalidGraphName(graphName, loadGML);
}

void CypherAnalyzer::analyze(LoadParquetQuery* loadParquet) {
    std::string_view graphName = loadParquet->getGraphName();
    if (graphName.empty()) {
        graphName = loadParquet->getFilePath().basename();
    }

    loadParquet->setGraphName(graphName);

    throwIfInvalidGraphName(graphName, loadParquet);
}

void CypherAnalyzer::analyze(const S3ConnectQuery* s3Connect) {
    const std::string_view accessId = s3Connect->getAccessId();
    const std::string_view secretKey = s3Connect->getSecretKey();
    const std::string_view region = s3Connect->getRegion();

    if (accessId.empty()) {
        throwError("S3 Access ID cannot be empty", s3Connect);
    }

    if (secretKey.empty()) {
        throwError("S3 Secret Key cannot be empty", s3Connect);
    }

    if (region.empty()) {
        throwError("S3 Region cannot be empty", s3Connect);
    }
}

void CypherAnalyzer::analyze(S3TransferQuery* s3Transfer) {
    std::string_view s3URL = s3Transfer->getS3Url();

    if (s3URL.substr(0, 5) != "s3://") {
        throwError(fmt::format("Invalid S3 URL: {}", s3URL));
    }
    s3URL.remove_prefix(5);
    const auto bucketEnd = s3URL.find('/');

    if (bucketEnd == std::string_view::npos) {
        throwError(fmt::format("S3 Bucket Not Found: {}", s3URL));
    }

    s3Transfer->setS3Bucket(s3URL.substr(0, bucketEnd));
    s3URL.remove_prefix(bucketEnd + 1);

    if (s3URL.empty()) {
        throwError(fmt::format("S3 Prefix/Folder not found: {}", s3URL));
    }

    if (s3URL.back() != '/') {
        // S3 'file' resource
        s3Transfer->setS3File(s3URL);
        return;
    }

    // S3 Directory Resource
    s3Transfer->setS3Prefix(s3URL);
}

void CypherAnalyzer::analyze(const CreateVectorIndexQuery* query) {
    const int64_t dimension = query->getDimension();
    if (dimension <= 0) {
        throwError("Vector index dimension must be greater than 0", query);
    }

    if (dimension > MAX_VECTOR_INDEX_DIMENSION) {
        throwError(fmt::format("Vector index dimension must not exceed {}", MAX_VECTOR_INDEX_DIMENSION), query);
    }

    const std::string_view indexName = query->getIndexName();
    if (indexName.empty()) {
        throwError("Vector index name cannot be empty", query);
    }
}

void CypherAnalyzer::analyze(const LoadVectorQuery* query) {
    const std::string_view filePath = query->getFilePath();
    if (filePath.empty()) {
        throwError("LOAD VECTOR file path cannot be empty", query);
    }

    const std::string_view indexName = query->getIndexName();
    if (indexName.empty()) {
        throwError("LOAD VECTOR index name cannot be empty", query);
    }
}

void CypherAnalyzer::analyze(const LoadEmbeddingQuery* query) {
    const std::string_view filePath = query->getFilePath();
    if (filePath.empty()) {
        throwError("LOAD EMBEDDING file path cannot be empty", query);
    }

    const std::string_view propertyName = query->getPropertyName();
    if (propertyName.empty()) {
        throwError("LOAD EMBEDDING property name cannot be empty", query);
    }
}

void CypherAnalyzer::analyze(const InstallExtensionQuery* query) {
    const std::string_view name = query->getExtensionName();
    if (name.empty()) {
        throwError("INSTALL extension name cannot be empty", query);
    }

    const size_t invalid = FileName::findInvalidCharacter(name);
    if (invalid != std::string_view::npos) [[unlikely]] {
        throwError(fmt::format("Extension name must only contain "
                               "alphanumeric characters or '_': "
                               "character '{}' not allowed.", name[invalid]),
                   query);
    }
}

void CypherAnalyzer::analyze(const CreateNodePropertyIndexQuery* query) {
    // CREATE INDEX _ FOR *(n)* ON n._
    const NodePattern* node = query->nodePattern();
    bioassert(node, "Failed to get node pattern.");

    const MapLiteral* properties = node->getProperties();
    const bool haveLabelConstraints = node->getLabelExpr() != nullptr;
    const bool havePropertyConstraints = properties && !properties->empty();
    const bool havePredicate = node->getWhere() != nullptr;
    if (haveLabelConstraints || havePropertyConstraints || havePredicate) {
        throwError("Constrained node indexes are not yet supported.", node);
    }
    PropertyExpr* propertyExpr = query->propertyExpr();
    bioassert(propertyExpr, "Failed to get property expression.");

    // Register an empty decl for the node of the query, so we can analyze the property
    // expression. The decl cannot be used anywhere aside from the property expr, so an
    // empty/invalid decl is fine.
    _exprAnalyzer->registerNodePatternDeclaration(node);
    _exprAnalyzer->analyzePropertyExpr(propertyExpr);
    _exprAnalyzer->throwIfReadsPartOfAProperty(propertyExpr);

    const PropertyTypeMap& propTypes = _graphMetadata.propTypes();
    const std::string_view propName = propertyExpr->getPropName();
    const std::optional<PropertyType> maybePropType = propTypes.get(propName);
    if (!maybePropType) {
        const std::string err = fmt::format("Property {} to index does not exist.", propName);
        throwError(std::move(err), propertyExpr);
    }

    const PropertyType propType = *maybePropType;
    const GraphReader reader = _graphView.read();
    if (!reader.isNodeProperty(propType._id)) {
        throwError("Property is not a node property.", propertyExpr);
    }
}

void CypherAnalyzer::analyze(const CreateEdgePropertyIndexQuery* query) {
    // CREATE INDEX _ FOR *[e]* ON e._
    const EdgePattern* edge = query->edgePattern();
    bioassert(edge, "Failed to get edge pattern.");

    const MapLiteral* properties = edge->getProperties();
    const bool haveTypeConstraints = edge->getLabelExpr() != nullptr;
    const bool havePropertyConstraints = properties && !properties->empty();
    const bool havePredicate = edge->getWhere() != nullptr;
    if (haveTypeConstraints || havePropertyConstraints || havePredicate) {
        throwError("Constrained edge indexes are not yet supported.", edge);
    }
    PropertyExpr* propertyExpr = query->propertyExpr();
    bioassert(propertyExpr, "Failed to get property expression.");

    // Register an empty decl for the edge of the query, so we can analyze the property
    // expression. The decl cannot be used anywhere aside from the property expr, so an
    // empty/invalid decl is fine.
    _exprAnalyzer->registerEdgePatternDeclaration(edge);
    _exprAnalyzer->analyzePropertyExpr(propertyExpr);
    _exprAnalyzer->throwIfReadsPartOfAProperty(propertyExpr);

    const PropertyTypeMap& propTypes = _graphMetadata.propTypes();
    const std::string_view propName = propertyExpr->getPropName();
    const std::optional<PropertyType> maybePropType = propTypes.get(propName);
    if (!maybePropType) {
        const std::string err = fmt::format("Property {} to index does not exist.", propName);
        throwError(std::move(err), propertyExpr);
    }

    const PropertyType propType = *maybePropType;
    const GraphReader reader = _graphView.read();
    if (!reader.isEdgeProperty(propType._id)) {
        throwError("Property is not an edge property.", propertyExpr);
    }
}

void CypherAnalyzer::throwIfInvalidGraphName(std::string_view graphName, const void* obj) const {
    const size_t invalid = FileName::findInvalidCharacter(graphName);
    if (invalid != std::string_view::npos) [[unlikely]] {
        throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': character '{}' not allowed.", graphName[invalid]), obj);
    }
}
