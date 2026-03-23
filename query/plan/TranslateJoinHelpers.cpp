#include "TranslateJoinHelpers.h"

#include "BioAssert.h"
#include "PlannerException.h"
#include "dataframe/Dataframe.h"
#include "interfaces/PipelineOutputInterface.h"
#include "nodes/JoinNode.h"

#include "Overloaded.h"

using namespace db;

ColumnTag TranslateJoinHelpers::getStreamTag(const PipelineOutputInterface* input) {
    const auto visitor = Overloaded {
        [](const EntityOutputStream::NodeStream& s) { return s._nodeIDsTag; },
        [](const EntityOutputStream::EdgeStream& s) { return s._edgeIDsTag; }};
    return input->getStream().visit(visitor);
}

// Merge the join tag with the node tag in node stream and edge id tag in edge stream
// this is needed when the stream contains a column involved in a join which invalidates
// it's column tag
EntityOutputStream TranslateJoinHelpers::mergeStream(const EntityOutputStream& stream,
                                                     ColumnTag joinTag) {
    const auto visitor = Overloaded {
        [](const EntityOutputStream::NodeStream&, ColumnTag tag) {
            return EntityOutputStream::createNodeStream(tag);
        },
        [](const EntityOutputStream::EdgeStream& s, ColumnTag tag) {
            return EntityOutputStream::createEdgeStream(tag, s._otherIDsTag, s._edgeTypesTag);
        }};
    return stream.visit(visitor, joinTag);
}

std::pair<ColumnTag, ColumnTag> TranslateJoinHelpers::getJoinKeyTags(const JoinNode* node,
                                                                     const PipelineOutputInterface* lhs,
                                                                     const PipelineOutputInterface* rhs,
                                                                     const DeclToTagMap& declMap) {
    ColumnTag leftJoinTag;
    ColumnTag rightJoinTag;

    switch (node->getJoinType()) {
        // In the common ancestor case the join var decls will exist in
        // the decl to column map so we can just find them
        case JoinType::COMMON_ANCESTOR: {
            // Both the first and second join keys are the same in this case
            const VarDecl* joinKey = node->getFirstJoinKey();
            auto leftJoinIt = declMap.find(joinKey);
            bioassert(leftJoinIt != declMap.end(), "Common Ancestor Join Key Not Found");

            auto rightJoinIt = declMap.find(joinKey);
            bioassert(rightJoinIt != declMap.end(), "Common Ancestor Join Key Not Found");

            leftJoinTag = leftJoinIt->second;
            rightJoinTag = rightJoinIt->second;

            break;
        }
        // In the common sucessor case the streams going into the join node will have
        // the unamed VarDecl's that correspond to the join keys
        case JoinType::COMMON_SUCCESSOR: {
            leftJoinTag = getStreamTag(lhs);
            rightJoinTag = getStreamTag(rhs);
            break;
        }
        case JoinType::PREDICATE: {
            // Check which input the dependency VarDecl is in this is should
            // be the fully expanded branch ( we will refer to this as the dependency branch)
            bool rhsIsDependencyBranch = true;

            const auto firstKeyIt = declMap.find(node->getFirstJoinKey());
            const auto secondKeyIt = declMap.find(node->getSecondJoinKey());
            bioassert(secondKeyIt != declMap.end(), "Second Predicate Join Var Decl Not Found In Decl Map");

            if (lhs->getDataframe()->hasColumn(secondKeyIt->second)) {
                rhsIsDependencyBranch = false;
            }

            // The second join key is on the dependency branch that
            // has been fully expanded.

            // If the First Join Key VarDecl has not been produced we are in an EntityID
            // predicate join.
            if (firstKeyIt == declMap.end()) {
                // the stream of the unexpanded branch contains the join tag
                if (rhsIsDependencyBranch) {
                    leftJoinTag = getStreamTag(lhs);
                    rightJoinTag = secondKeyIt->second;
                } else {
                    rightJoinTag = getStreamTag(rhs);
                    leftJoinTag = secondKeyIt->second;
                }
            } else {
                // In the property predicate join the join tags have already been
                // produced in the pipeline. We know that the second join key belongs
                // to the fully expanded branch (by convention).
                if (rhsIsDependencyBranch) {
                    leftJoinTag = firstKeyIt->second;
                    rightJoinTag = secondKeyIt->second;
                } else {
                    leftJoinTag = secondKeyIt->second;
                    rightJoinTag = firstKeyIt->second;
                }
            }
            break;
        }
        case JoinType::DIAMOND: {
            throw PlannerException("Common Successor Joins With Common Ancestor Unsupported");
        }
    }
    return {leftJoinTag, rightJoinTag};
}

// Set the appropriate stream - we only care about this in these Mutally Exclusive cases:
//  - When we have a common sucessor join : The streams coming out of the common sucessor
//    node's temporary VarDecls need to be merged with the join tag appropriately
//
//     Cypher Example: match (a)-->(c), (b)-->(c) return a,b,c
//
//  - When we have a dependency VarDecl , there are two cases of this :
//           - When we have a dataflow dependency join - here the dependency branch
//             is fully expanded so the stream that propagates from the join should
//             be the stream of the unexpanded branch
//
//             Cypher Example: match (x)-->(a), (x)-->(b) where a.age = b.age return x
//
//           - When we have a predicate join, there are two cases of this as well:
//                       - Property Predicate Join - in this case we just need to propogate
//                         the undexpanded branch.
//
//                         Cypher Example : match (x)-->(a), (y)-->(b) where x.age = b.age
//
//                       - Entity ID Predicate Join - in this case the unexpanded branch's
//                         stream would contain the temporary VarDecl (of the Entity dep)
//                         whose values would be written into the new join column. So the
//                         stream we propagate has to be merged with join tag.
//
//                         Cypher Example : match (x)-[e1]->(a)-->(d), (y)-[e2]->(b) where e1 = e2
//                                          or
//                         Cypher Example : match (x)-[e1]->(a)-->(d), (y)-[e2]->(b) where x = b
//
//  In all other cases (regular Common Ancestor case) we don't care about the stream
//  as the join happens as the last processor in the pipeline
EntityOutputStream TranslateJoinHelpers::resolveStream(const JoinNode* node,
                                                       const PipelineOutputInterface* lhs,
                                                       const PipelineOutputInterface* rhs,
                                                       ColumnTag joinTag,
                                                       DeclToTagMap& declMap) {
    // Default value is to push random stream
    EntityOutputStream stream = lhs->getStream();

    if (node->getDependencyVarDecl()) {
        const auto it = declMap.find(node->getDependencyVarDecl());
        if (it == declMap.end()) {
            throw PlannerException("Join Dependency VarDecl Not Found");
        }
        if (!lhs->getDataframe()->hasColumn(it->second)) {
            // the decl column wouldn't contain the first join key only
            // in the EntityID predicate join case
            if (!declMap.contains(node->getFirstJoinKey())) {
                stream = mergeStream(lhs->getStream(), joinTag);
            } else {
                stream = lhs->getStream();
            }
        } else {
            if (!declMap.contains(node->getFirstJoinKey())) {
                stream = mergeStream(rhs->getStream(), joinTag);
            } else {
                stream = rhs->getStream();
            }
        }
    }

    // In the Property Predicate Join case if the properties are referred to
    // after the join then we need to make sure that the VarDecl's point to the appropriate
    // column. In the EntityID predicate join case - the expanded branches var decl mapping
    // would need to be changed, the unexpanded branch hasn't produced the join key VarDecl
    // yet, the mapping we create below would be over written so it is just a noop
    if (node->getJoinType() == JoinType::PREDICATE) {
        declMap.insert_or_assign(node->getFirstJoinKey(), joinTag);
        declMap.insert_or_assign(node->getSecondJoinKey(), joinTag);
    }

    if (node->getJoinType() == JoinType::COMMON_SUCCESSOR) {
        stream = mergeStream(stream, joinTag);
    }

    return stream;
}
