#include "StringIndexLoader.h"

#include <capnp/message.h>
#include <capnp/serialize.h>
#include <cassert>
#include <unistd.h>

#include "FileUtils.h"
#include "StringIndex.h"
#include "capnp/StringIndex.capnp.h"

#include "MsgCommon.h"
#include "MsgDB.h"
#include "BioLog.h"

using namespace Log;

namespace db {

StringIndexLoader::StringIndexLoader(const Path& dbPath)
    : _indexPath(dbPath)
{
}

bool StringIndexLoader::load(StringIndex& index) {

    index.clear();
    _stringIdMapping.clear();

    if (!FileUtils::exists(_indexPath)) {
        BioLog::log(msg::ERROR_FILE_NOT_EXISTS() << _indexPath.string());
        return false;
    }

    const int indexFD = FileUtils::openForRead(_indexPath);
    if (indexFD < 0) {
        BioLog::log(msg::ERROR_FAILED_TO_OPEN_FOR_READ() << _indexPath.string());
        return false;
    }

    //::capnp::PackedFdMessageReader message(indexFD);
    ::capnp::StreamFdMessageReader message(indexFD, {
        .traversalLimitInWords = 100000000000000,
        .nestingLimit = 128,
    });
    const auto stringReader = message.getRoot<OnDisk::StringIndex>();

    _stringIdMapping.resize(stringReader.getStrings().size());

    for (OnDisk::SharedString::Reader s : stringReader.getStrings()) {
        const StringRef newString = index.insertString(s.getStr(), (DBIndex)s.getId());
        _stringIdMapping[s.getId()] = newString;
    }

    close(indexFD);
    return true;
}
}
