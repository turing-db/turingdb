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

    auto stringIndexBuilder = message.initRoot<OnDisk::StringIndex>();
    auto stringListBuilder = stringIndexBuilder.initStrings(index._strMap.size());

    const int indexFD = FileUtils::openForWrite(_indexPath);
    if (indexFD < 0) {
        return false;
    }

    // Looping through all strings in the StringIndex
    for (const auto& [rstr, sstr] : index._strMap) {
        stringListBuilder[sstr->getID()].setId(sstr->getID());
        stringListBuilder[sstr->getID()].setStr(rstr);
    }

    ::capnp::writePackedMessageToFd(indexFD, message);
    close(indexFD);

    return true;
}

}
