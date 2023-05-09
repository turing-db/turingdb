#pragma once

namespace db::Serializer {

enum class Result {
    SUCCESS = 0,
    CANT_ACCESS_OUT_DIR,
    CANT_ACCESS_DUMP_DIR,
    CANT_ACCESS_STRING_INDEX_FILE,
};

}
