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

    int stringIndexFD = FileUtils::openForRead(_indexPath);

    if (stringIndexFD < 0) {
        return false;
    }

    ::capnp::PackedFdMessageReader message(stringIndexFD);
    OnDisk::StringIndex::Reader strings =
        message.getRoot<OnDisk::StringIndex>();

    stringRefs.resize(strings.getStrings().size());

    for (OnDisk::SharedString::Reader s : strings.getStrings()) {
        StringRef newString = index.insertString(s.getStr(), s.getId());
        stringRefs[s.getId()] = newString;
    }

    close(stringIndexFD);
    return true;
}
}
