#include "EmbeddingsSpec.h"

#include <string_view>

using namespace db;

EmbeddingsSpec::Iterator EmbeddingsSpec::find(std::string_view propName) {
    return _specMap.find(propName);
}

EmbeddingsSpec::ConstIterator EmbeddingsSpec::find(std::string_view propName) const {
    return _specMap.find(propName);
}
