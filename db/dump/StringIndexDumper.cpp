#include "StringIndexDumper.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndex.h"
#include "BioLog.h"
#include "capnp/StringIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <cassert>
#include <unistd.h>

namespace db {

StringIndexDumper::StringIndexDumper(const Path& indexPath)
    : _indexPath(indexPath)
{
}

bool StringIndexDumper::dump(const StringIndex& index) {
    // Remove smap file if it already exists
    if (FileUtils::exists(_indexPath)) {
        if (!FileUtils::removeFile(_indexPath)) {
            Log::BioLog::log(msg::ERROR_FAILED_TO_REMOVE_FILE() << _indexPath);
            return false;
        }
    }

    ::capnp::MallocMessageBuilder message;

    OnDisk::StringIndex::Builder strings =
        message.initRoot<OnDisk::StringIndex>();

    ::capnp::List<OnDisk::SharedString>::Builder listBuilder =
        strings.initStrings(index._strMap.size());

    int stringIndexFD = FileUtils::openForWrite(_indexPath);
    if (stringIndexFD < 0) {
        return false;
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

    return true;
}

}
