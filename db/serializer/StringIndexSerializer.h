#pragma once

#include "SerializerResult.h"
#include <filesystem>

namespace db {

class DB;
class StringIndex;

class StringIndexSerializer {
public:
    using Path = std::filesystem::path;
    StringIndexSerializer(const Path& dbPath);

    Serializer::Result load(StringIndex& index);
    Serializer::Result dump(const StringIndex& index);

private:
    Path _indexPath;
};

}
