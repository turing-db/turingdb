#include "StringIndexSerializer.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndex.h"
#include "capnp_src/StringIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <cassert>
#include <unistd.h>

namespace db {

StringIndexSerializer::StringIndexSerializer(const Path& dbPath)
    : _indexPath(dbPath) //
{                        //
}

Serializer::Result StringIndexSerializer::load(StringIndex& index) {
    index.clear();

    if (!FileUtils::exists(_indexPath)) {
        return Serializer::Result::CANT_ACCESS_STRING_INDEX_FILE;
    }

    int stringIndexFD = FileUtils::openForRead(_indexPath);

    if (stringIndexFD < 0) {
        return Serializer::Result::CANT_ACCESS_STRING_INDEX_FILE;
    }

    ::capnp::PackedFdMessageReader message(stringIndexFD);
    OnDisk::StringIndex::Reader strings =
        message.getRoot<OnDisk::StringIndex>();

    for (OnDisk::SharedString::Reader s : strings.getStrings()) {
        index.insertString(s.getStr(), s.getId());
    }

    close(stringIndexFD);
    return Serializer::Result::SUCCESS;
}

Serializer::Result StringIndexSerializer::dump(const StringIndex& index) {
    ::capnp::MallocMessageBuilder message;

    OnDisk::StringIndex::Builder strings =
        message.initRoot<OnDisk::StringIndex>();

    ::capnp::List<OnDisk::SharedString>::Builder listBuilder =
        strings.initStrings(index._strMap.size());

    int stringIndexFD = FileUtils::openForWrite(_indexPath);
    if (stringIndexFD < 0) {
        return Serializer::Result::CANT_ACCESS_STRING_INDEX_FILE;
    }

    // Looping through all strings in the StringIndex
    size_t i = 0;
    for (const auto& [rstr, sstr] : index._strMap) {
        OnDisk::SharedString::Builder onDisk = listBuilder[i];
        onDisk.setId(sstr->getID());
        onDisk.setStr(rstr);
        i++;
    }

    ::capnp::writePackedMessageToFd(stringIndexFD, message);
    close(stringIndexFD);

    return Serializer::Result::SUCCESS;
}

}
