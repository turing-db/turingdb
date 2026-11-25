#include "DBProgramGenerator.h"

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/IR/Value.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "DBBoundColumns.h"
#include "DBHiddenColumnNames.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "decl/VarDecl.h"

#include "BioAssert.h"

using namespace db;

void DBProgramGenerator::rebindYieldedColumn(const VarDecl* decl, mlir::TypedValue<mlir::Type> val) {
    for (YieldedColumn& yielded : _part._yieldedColumns) {
        if (yielded._decl == decl) {
            yielded._column = val;
        }
    }
}

void DBProgramGenerator::rebindNamedPath(const VarDecl* decl, mlir::Value column) {
    if (!_part._namedPaths.contains(decl)) {
        return;
    }

    _part._namedPaths[decl] = column;
}

void DBProgramGenerator::rebindGroupVariable(const VarDecl* decl, mlir::Value column) {
    const auto bindingIt = _part._pathBindings.find(decl);
    if (bindingIt == _part._pathBindings.end()) {
        return;
    }

    const VariableDependency* walk = bindingIt->second._path;
    const bool readOffAWalk = walk && walk->getDecl() != decl;
    if (!readOffAWalk) {
        return;
    }

    // The column holds the group's list now, no longer read off the walk's paths
    _part._pathBindings.erase(bindingIt);

    const VariableDependency* var = _vdg.registerBoundVariable(decl->getName(), decl);
    registerValue(var, column);
}

mlir::Value DBProgramGenerator::findYieldedColumn(const VarDecl* decl) const {
    if (!decl) {
        return mlir::Value {};
    }

    for (const YieldedColumn& yielded : _part._yieldedColumns) {
        if (yielded._decl == decl) {
            return yielded._column;
        }
    }

    return mlir::Value();
}

std::string_view DBProgramGenerator::findCSVFieldRow(const VarDecl* decl) const {
    const auto foundIt = _csvFieldRows.find(decl);
    if (foundIt == end(_csvFieldRows)) {
        return {};
    }

    return foundIt->second;
}

bool DBProgramGenerator::holdsColumn(const VariableDependency* var) const {
    const auto findIt = _part._varMap.find(var);

    return findIt != _part._varMap.end() && !findIt->second.empty();
}

mlir::Value DBProgramGenerator::resolveEntityColumn(const VarDecl* decl) {
    // The element a list comprehension bound rides the block argument of its body, which
    // stands only while that body is translated
    const auto elementIt = _part._comprehensionElements.find(decl);
    if (elementIt != _part._comprehensionElements.end()) {
        return elementIt->second;
    }

    const auto hopIt = _part._hopColumns.find(decl);
    if (hopIt != end(_part._hopColumns)) {
        return hopIt->second;
    }

    for (const auto& [var, values] : _part._varMap) {
        if (var->getDecl() == decl && !values.empty()) {
            return values.back();
        }
    }

    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();
    const auto findIt = edgeIdentities.find(decl);
    const bool foundEdgeIdentity = findIt != edgeIdentities.end() && !findIt->second.empty();
    if (foundEdgeIdentity) {
        const VariableDependency* representative = findIt->second.front();
        return findVarOrThrow(_part._varMap, representative);
    }

    const auto createdIt = _part._createdEntities.find(decl);
    if (createdIt != _part._createdEntities.end()) {
        return createdIt->second._column;
    }

    const auto namedPathIt = _part._namedPaths.find(decl);
    if (namedPathIt != _part._namedPaths.end()) {
        return namedPathIt->second;
    }

    // A group variable of a quantified pattern is the list of one entity per repetition,
    // read off the paths of its walk
    const auto pathIt = _part._pathBindings.find(decl);
    if (pathIt != _part._pathBindings.end() && pathIt->second._path) {
        return listColumnOf(decl, findVarOrThrow(_part._varMap, pathIt->second._path));
    }

    const auto projectedIt = _part._projectedColumns.find(decl);
    if (projectedIt != _part._projectedColumns.end()) {
        return projectedIt->second;
    }

    return findYieldedColumn(decl);
}

void DBProgramGenerator::rebindScope(llvm::ArrayRef<PublishedColumn> published) {
    _part = PartScope {};
    _vdg.clear();

    llvm::SmallVector<const VariableDependency*> boundVars;
    for (const PublishedColumn& column : published) {
        bioassert(!column._name.empty(), "Bound column without a name");

        const VariableDependency* var = _vdg.registerBoundVariable(column._name, column._decl);
        registerValue(var, column._column);
        boundVars.push_back(var);
    }

    for (size_t maskIndex = 0; maskIndex < published.size(); maskIndex++) {
        const std::string_view maskName = published[maskIndex]._name;
        if (!maskName.starts_with(pendingMaskPrefix)) {
            continue;
        }

        const std::string_view entityName = maskName.substr(pendingMaskPrefix.size());
        const auto entityIt = std::ranges::find(published, entityName, &PublishedColumn::_name);
        bioassert(entityIt != published.end(), "Pending mask '{}' published without its entity", maskName);

        _part._pendingMasks[entityIt->_decl] = boundVars[maskIndex];
    }
}

void DBProgramGenerator::rebindScopeKeepingWrittenEntities(llvm::ArrayRef<PublishedColumn> published) {
    CarriedEntities carried;
    for (const PublishedColumn& column : published) {
        const PartScope::WrittenEntity* written = findWrittenEntity(column._decl);
        if (written) {
            carried.emplace_back(column._decl, *written);
        }
    }

    rebindScope(published);

    for (auto& [decl, written] : carried) {
        _part._writtenEntities[decl] = std::move(written);
    }
}

void DBProgramGenerator::collectPublishedColumns(llvm::SmallVectorImpl<PublishedColumn>& published) {
    forEachVariableColumn([&published](const VarDecl* decl, std::string_view name, mlir::Value column) {
        const auto sameName = [name](const PublishedColumn& candidate) {
            return candidate._name == name;
        };

        const auto foundIt = std::ranges::find_if(published, sameName);
        if (foundIt != published.end()) {
            // A pattern casting a variable binds its name to the cast, in place of the value
            const bool castsTheVariable = foundIt->_decl && foundIt->_decl->getCastSource() == decl;
            if (!castsTheVariable) {
                foundIt->_decl = decl;
                foundIt->_column = column;
            }

            return;
        }

        published.push_back({decl, std::string(name), column});
    });

    // Under a name order rather than the map's, so every branch of a union publishes its
    // columns in the one order the union's results are typed and read in
    std::ranges::sort(published, [](const PublishedColumn& left, const PublishedColumn& right) {
        return left._name < right._name;
    });

    for (PublishedColumn& column : published) {
        column._column = columnLeavingScope(column._decl, column._column);
    }
}

void DBProgramGenerator::publishProjectedAliases(llvm::SmallVectorImpl<PublishedColumn>& published) const {
    for (const auto& [decl, column] : _part._projectedColumns) {
        const bool publishedAlready = std::ranges::find(published, decl, &PublishedColumn::_decl) != published.end();

        if (decl->isUnnamed() || publishedAlready) {
            continue;
        }

        const std::string_view name = decl->getName();
        const PublishedColumn alias {decl, std::string(name), column};
        const auto shadowedIt = std::ranges::find(published, name, &PublishedColumn::_name);

        if (shadowedIt != published.end()) {
            *shadowedIt = alias;
        } else {
            published.push_back(alias);
        }
    }

    std::ranges::sort(published, {}, &PublishedColumn::_name);
}

void DBProgramGenerator::publishInFlightColumns() {
    llvm::SmallVector<PublishedColumn> published;
    collectPublishedColumns(published);

    rebindScopeKeepingWrittenEntities(published);
}

void DBProgramGenerator::forEachVariableColumn(const VariableColumnBinding& bind) const {
    const auto namesTheWalk = [](const VarDecl* groupDecl, const PartScope::PathBinding& binding) {
        return !binding._path || binding._path->getDecl() == groupDecl;
    };

    // An occurrence of a named edge, and the walk a group variable stands for, holds a column
    // the query names below, and is bound under that name alone
    llvm::SmallPtrSet<const VariableDependency*, 8> namedBelow;
    for (const auto& [decl, vars] : _vdg.edgeIdentities()) {
        if (!vars.empty() && holdsColumn(vars.front())) {
            namedBelow.insert(vars.begin(), vars.end());
        }
    }

    for (const auto& [groupDecl, binding] : _part._pathBindings) {
        if (!namesTheWalk(groupDecl, binding)) {
            namedBelow.insert(binding._path);
        }
    }

    for (const auto& [cypherVar, mlirCol] : _part._varMap) {
        if (namedBelow.contains(cypherVar)) {
            continue;
        }

        const std::string_view varName = cypherVar->getName();

        bioassert(not mlirCol.empty(), "No definitions for {}", varName);

        bind(cypherVar->getDecl(), varName, mlirCol.back());
    }

    // A CALL's yielded columns are variables of the query too, declared by the YIELD.
    for (const YieldedColumn& yieldedColumn : _part._yieldedColumns) {
        bind(yieldedColumn._decl, yieldedColumn._name, yieldedColumn._column);
    }

    // So are the entities a CREATE wrote, declared by its pattern
    for (const auto& [createdDecl, created] : _part._createdEntities) {
        bind(createdDecl, createdDecl->getName(), created._column);
    }

    // So is the path a `MATCH p = ...` named, declared by its pattern element
    for (const auto& [pathDecl, pathColumn] : _part._namedPaths) {
        bind(pathDecl, pathDecl->getName(), pathColumn);
    }

    // So is each group variable of a quantified pattern, another name for the handle column
    // of its walk
    for (const auto& [groupDecl, binding] : _part._pathBindings) {
        if (namesTheWalk(groupDecl, binding)) {
            continue;
        }

        bind(groupDecl, groupDecl->getName(), findVarOrThrow(_part._varMap, binding._path));
    }

    // And so is the element a list comprehension bound, inside its body
    for (const auto& [elementDecl, element] : _part._comprehensionElements) {
        bind(elementDecl, elementDecl->getName(), element);
    }

    for (const auto& [decl, vars] : _vdg.edgeIdentities()) {
        bioassert(!vars.empty(), "Empty edge identity for '{}'", decl->getName());

        const VariableDependency* representative = vars.front();
        if (!holdsColumn(representative)) {
            continue;
        }

        bind(decl, decl->getName(), findVarOrThrow(_part._varMap, representative));
    }
}

void DBProgramGenerator::collectVariableColumns(VariableColumnMap& variableColumns) const {
    forEachVariableColumn([&variableColumns](const VarDecl* decl, std::string_view name, mlir::Value column) {
        if (decl) {
            variableColumns[decl] = column;
        }
    });
}

mlir::Value DBProgramGenerator::findVariableColumn(const VarDecl* decl) const {
    if (!decl) {
        return mlir::Value {};
    }

    mlir::Value found;
    forEachVariableColumn([&found, decl](const VarDecl* boundDecl, std::string_view name, mlir::Value column) {
        if (boundDecl == decl) {
            found = column;
        }
    });

    return found;
}

void DBProgramGenerator::rebindVariableColumn(const VarDecl* decl, mlir::Value column) {
    rebindYieldedColumn(decl, column);
    rebindNamedPath(decl, column);
    rebindGroupVariable(decl, column);

    // An edge identity has no variable of its own: its column is published under the
    // traversal variable that produced it, which is where the replacement has to land
    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();
    const auto identityIt = edgeIdentities.find(decl);
    const bool hasIdentity = identityIt != edgeIdentities.end() && !identityIt->second.empty();

    const VariableDependency* boundVar = nullptr;

    if (hasIdentity) {
        boundVar = identityIt->second.front();
    } else {
        for (const auto& [var, values] : _part._varMap) {
            if (var->getDecl() == decl) {
                boundVar = var;
                break;
            }
        }
    }

    if (!boundVar) {
        return;
    }

    registerValue(boundVar, column);

    // The edge types published for the variable were read over the rows this column
    // replaces, so they no longer line up with it: dropping them has the next read fetch
    // them over this column instead
    _part._edgeTypeMap.erase(boundVar);
}
