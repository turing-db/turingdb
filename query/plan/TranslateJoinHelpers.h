#pragma once

#include <unordered_map>
#include <utility>

#include "EntityOutputStream.h"
#include "dataframe/ColumnTag.h"


namespace db {
class JoinNode;
class VarDecl;
class PipelineOutputInterface;

class TranslateJoinHelpers {
public:
    TranslateJoinHelpers() = delete;
    ~TranslateJoinHelpers() = delete;

    using DeclToTagMap = std::unordered_map<const VarDecl*, ColumnTag>;

    static ColumnTag getStreamTag(const PipelineOutputInterface* input);
    static EntityOutputStream mergeStream(const EntityOutputStream& stream, ColumnTag joinTag);

    static std::pair<ColumnTag, ColumnTag> getJoinKeyTags(const JoinNode* node,
                                                          const PipelineOutputInterface* lhs,
                                                          const PipelineOutputInterface* rhs,
                                                          const DeclToTagMap& declMap);

    static EntityOutputStream resolveStream(const JoinNode* node,
                                            const PipelineOutputInterface* lhs,
                                            const PipelineOutputInterface* rhs,
                                            ColumnTag joinTag,
                                            DeclToTagMap& declMap);
};
}
