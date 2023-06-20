#include "StringIndexLoader.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndex.h"
#include "capnp/StringIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <cassert>
#include <unistd.h>

namespace db {

StringIndexLoader::StringIndexLoader(const Path& dbPath)
    : _indexPath(dbPath)
{
}

bool StringIndexLoader::load(StringIndex& index,
                             std::vector<StringRef>& stringRefs) {

    index.clear();
    stringRefs.clear();

    if (!FileUtils::exists(_indexPath)) {
        return false;
    }

    const int indexFD = FileUtils::openForRead(_indexPath);

    if (indexFD < 0) {
        return false;
    }

    ::capnp::PackedFdMessageReader message(indexFD);
    const auto stringReader = message.getRoot<OnDisk::StringIndex>();

    stringRefs.resize(stringReader.getStrings().size());

    for (OnDisk::SharedString::Reader s : stringReader.getStrings()) {
        const StringRef newString = index.insertString(s.getStr(), s.getId());
        stringRefs[s.getId()] = newString;
    }

    close(indexFD);
    return true;
}
}
