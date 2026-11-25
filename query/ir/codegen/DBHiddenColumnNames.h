#pragma once

#include <string>
#include <string_view>

namespace db {

// The row tag an OPTIONAL MATCH carries beside the columns its pattern walks. A backtick
// can appear in no Cypher identifier, quoted or not, so no variable of the query's own is
// ever taken for the tag - the columns are keyed by name.
constexpr std::string_view optionalTagName {"`optional_tag"};

// The row tag a pattern comprehension carries beside the columns its pattern walks, under
// a name of the same shape and for the same reason as the one above.
constexpr std::string_view comprehensionTagName {"`comprehension_tag"};

std::string hiddenName(std::string_view name);

bool isHiddenName(std::string_view name);

// The row tag an OPTIONAL CALL body carries, under a hidden name no input's own hidden
// name can be: an identifier holds no backtick
constexpr std::string_view subqueryTagName {"`hidden_`tag"};

// The row tag an EXISTS body carries, under a hidden name no clause of the body resolves
// or drops - a barrier inside it carries the column on as it does a CALL's tag
constexpr std::string_view existsTagName {"`hidden_`exists_tag"};

constexpr std::string_view countTagName {"`hidden_`count_tag"};

constexpr std::string_view collectTagName {"`hidden_`collect_tag"};

// What a single query of an EXISTS or COUNT body returning nothing yields when the body is a
// UNION or a WHEN: one constant column, so the op holding its branches has rows to count
constexpr std::string_view ANSWER_COLUMN_NAME {"`hidden_`answer"};

// The mask of an entity a MERGE wrote is bound past a WITH under the entity's name behind
// this prefix. No hidden name starts with it, so a barrier publishing no entity drops it
constexpr std::string_view pendingMaskPrefix {"`pending_"};

std::string pendingMaskName(std::string_view entityName);

}
