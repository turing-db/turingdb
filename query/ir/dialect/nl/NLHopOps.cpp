#include "NLOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "NLChunkTypes.h"

using namespace mlir;
using namespace mlir::nl;

// An out-edges fetch produces one row of edge chunks per step, then one
// filtered chunk per carried column, mirroring db.get_out_edges
LogicalResult GetOutEdges::inferReturnTypes(MLIRContext* context,
                                            std::optional<Location> location,
                                            GetOutEdges::Adaptor adaptor,
                                            SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// An in-edges fetch produces the same row of edge chunks as out-edges, plus the
// same carried chunks; the input chunk names the nodes whose in-edges are gathered
LogicalResult GetInEdges::inferReturnTypes(MLIRContext* context,
                                           std::optional<Location> location,
                                           GetInEdges::Adaptor adaptor,
                                           SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

LogicalResult GetEdges::inferReturnTypes(MLIRContext* context,
                                         std::optional<Location> location,
                                         GetEdges::Adaptor adaptor,
                                         SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// Filtering out-edges by type narrows only the rows, never their shape, so a
// by-type fetch produces the same iterator as GetOutEdges - the edge type name is
// a filter, not a result type.
LogicalResult GetOutEdgesByType::inferReturnTypes(MLIRContext* context,
                                                  std::optional<Location> location,
                                                  GetOutEdgesByType::Adaptor adaptor,
                                                  SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// The in-edge by-type sibling: same iterator shape as GetInEdges, narrowed by type.
LogicalResult GetInEdgesByType::inferReturnTypes(MLIRContext* context,
                                                 std::optional<Location> location,
                                                 GetInEdgesByType::Adaptor adaptor,
                                                 SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// The by-label hops: same iterator shape again, narrowed by the label set of the endpoint
// the hop reaches.
LogicalResult GetOutEdgesByLabel::inferReturnTypes(MLIRContext* context,
                                                   std::optional<Location> location,
                                                   GetOutEdgesByLabel::Adaptor adaptor,
                                                   SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

LogicalResult GetInEdgesByLabel::inferReturnTypes(MLIRContext* context,
                                                  std::optional<Location> location,
                                                  GetInEdgesByLabel::Adaptor adaptor,
                                                  SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

LogicalResult GetOutEdgesByTypeAndLabel::inferReturnTypes(MLIRContext* context,
                                                          std::optional<Location> location,
                                                          GetOutEdgesByTypeAndLabel::Adaptor adaptor,
                                                          SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

LogicalResult GetInEdgesByTypeAndLabel::inferReturnTypes(MLIRContext* context,
                                                         std::optional<Location> location,
                                                         GetInEdgesByTypeAndLabel::Adaptor adaptor,
                                                         SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}
