#pragma once

#include "PlanGraphNode.h"
#include "metadata/PropertyType.h"

namespace db {
class VarDecl;

class MultiSourceShortestPathNode : public PlanGraphNode {
public:
    MultiSourceShortestPathNode(PlanGraphNodeID id,
                                const VarDecl* source,
                                const VarDecl* target,
                                const VarDecl* sourceOutput,
                                const VarDecl* targetOutput,
                                const VarDecl* distance,
                                const VarDecl* path,
                                const PropertyType edgeType)
        : PlanGraphNode(id, PlanGraphOpcode::MULTI_SOURCE_SHORTEST_PATH),
        _source(source),
        _target(target),
        _sourceOutput(sourceOutput),
        _targetOutput(targetOutput),
        _distance(distance),
        _path(path),
        _edgeType(edgeType)
    {
    }

    const VarDecl* getSource() { return _source; };
    const VarDecl* getTarget() { return _target; };
    const VarDecl* getSourceOutput() { return _sourceOutput; };
    const VarDecl* getTargetOutput() { return _targetOutput; };
    const VarDecl* getDistance() { return _distance; };
    const VarDecl* getPath() { return _path; };
    const PropertyType& getEdgeType() { return _edgeType; };

private:
    const VarDecl* _source {nullptr};
    const VarDecl* _target {nullptr};
    const VarDecl* _sourceOutput {nullptr};
    const VarDecl* _targetOutput {nullptr};
    const VarDecl* _distance {nullptr};
    const VarDecl* _path {nullptr};
    const PropertyType _edgeType;
};

}
