#include "NLTranslator.h"

#include <optional>
#include <string_view>

#include <spdlog/fmt/bundled/format.h>

#include "NLOps.h"
#include "ParameterValue.h"
#include "StorageAttributes.h"

#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/SmallVector.h"

#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "NLWriteProperties.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

bool NLTranslator::resolveLabelSet(llvm::ArrayRef<llvm::StringRef> labels, LabelSet& labelset) const {
    for (const llvm::StringRef label : labels) {
        const std::optional<LabelID> id = findLabel(label);
        if (!id) {
            return false;
        }

        labelset.set(*id);
    }

    return true;
}

void NLTranslator::resolveLabelNames(mlir::ArrayAttr labels, llvm::SmallVectorImpl<llvm::StringRef>& names) const {
    resolveSchemaNames(labels, LABEL_NOUNS, names);
}

void NLTranslator::resolveEdgeTypeNames(mlir::ArrayAttr edgeTypes, llvm::SmallVectorImpl<llvm::StringRef>& names) const {
    resolveSchemaNames(edgeTypes, EDGE_TYPE_NOUNS, names);
}

// A by-type hop carries the nl.get_edge_type_set handle, not the names: they sit on the
// handle op, resolved once above the loops, and a hop reads them back through its operand
void NLTranslator::resolveEdgeTypeNames(mlir::Value handle, llvm::SmallVectorImpl<llvm::StringRef>& names) const {
    nl::GetEdgeTypeSet handleOp = handle.getDefiningOp<nl::GetEdgeTypeSet>();
    if (!handleOp) {
        throw IRException("edge_types operand must come from nl.get_edge_type_set");
    }

    resolveEdgeTypeNames(handleOp.getNames(), names);
}

llvm::StringRef NLTranslator::resolveEdgeTypeName(mlir::Attribute edgeType) const {
    if (const mlir::StringAttr name = mlir::dyn_cast<mlir::StringAttr>(edgeType)) {
        return name.getValue();
    }

    const mlir::storage::ParameterAttr parameter = mlir::cast<mlir::storage::ParameterAttr>(edgeType);
    const llvm::StringRef name = parameter.getName();
    const ParameterValue& value = lookupParameter(name);

    if (value.isNull()) {
        throw IRException(fmt::format("Parameter ${} is null: an edge is written with one edge type, a String", name));
    }

    const ValueType type = value.getType();
    if (type != ValueType::String) {
        throw IRException(fmt::format("Parameter ${} has type {}: an edge is written with one edge type, a String",
                                      name,
                                      ValueTypeName::value(type)));
    }

    return boundSchemaName(name, EDGE_TYPE_NOUNS, value.getString());
}

void NLTranslator::resolveSchemaNames(mlir::ArrayAttr refs,
                                      const SchemaNameNouns& nouns,
                                      llvm::SmallVectorImpl<llvm::StringRef>& names) const {
    for (const mlir::Attribute ref : refs) {
        if (const mlir::StringAttr name = mlir::dyn_cast<mlir::StringAttr>(ref)) {
            names.push_back(name.getValue());
        } else {
            bindParameterSchemaNames(mlir::cast<mlir::storage::ParameterAttr>(ref), nouns, names);
        }
    }
}

// What this change knows a label by: the graph's schema, plus the names a CREATE earlier in
// the program introduced, which live in the change's own schema and nowhere else until the
// commit. The edge type sibling of this is findEdgeType.
std::optional<LabelID> NLTranslator::findLabel(llvm::StringRef name) const {
    const std::optional<LabelID> labelID = _view->metadata().labels().get(name);
    if (labelID || !_metadataBuilder) {
        return labelID;
    }

    return _metadataBuilder->findLabel(name);
}

std::optional<PropertyType> NLTranslator::findPropertyType(llvm::StringRef name) const {
    const std::string_view propertyName(name.data(), name.size());

    return findWrittenPropertyType(_view, _metadataBuilder, propertyName);
}

std::optional<EdgeTypeID> NLTranslator::findEdgeType(llvm::StringRef name) const {
    const std::optional<EdgeTypeID> edgeTypeID = _view->metadata().edgeTypes().get(name);
    if (edgeTypeID || !_metadataBuilder) {
        return edgeTypeID;
    }

    return _metadataBuilder->findEdgeType(name);
}

void NLTranslator::resolveEdgeTypes(llvm::ArrayRef<llvm::StringRef> names,
                                    llvm::SmallVectorImpl<EdgeTypeID>& resolved) const {
    for (const llvm::StringRef name : names) {
        const std::optional<EdgeTypeID> edgeTypeID = findEdgeType(name);
        if (edgeTypeID) {
            resolved.push_back(*edgeTypeID);
        }
    }
}

void NLTranslator::resolveEdgeTypes(mlir::ArrayAttr names,
                                    llvm::SmallVectorImpl<EdgeTypeID>& resolved) const {
    llvm::SmallVector<llvm::StringRef, 4> nameRefs;
    resolveEdgeTypeNames(names, nameRefs);

    resolveEdgeTypes(nameRefs, resolved);
}
