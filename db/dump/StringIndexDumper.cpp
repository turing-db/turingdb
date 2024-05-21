#include "StringIndexDumper.h"

#include <capnp/message.h>
#include <capnp/serialize.h>
#include <cassert>
#include <unistd.h>

#include "FileUtils.h"
#include "StringIndex.h"
#include "capnp/StringIndex.capnp.h"
#include "LogUtils.h"

namespace db {

StringIndexDumper::StringIndexDumper(const Path& indexPath)
    : _indexPath(indexPath)
{
}

bool StringIndexDumper::dump(const StringIndex& index) {
    // Remove smap file if it already exists
    if (FileUtils::exists(_indexPath)) {
        if (!FileUtils::removeFile(_indexPath)) {
            logt::CanNotRemove(_indexPath.string()); 
            return false;
        }
    }

    ::capnp::MallocMessageBuilder message;

    auto stringIndexBuilder = message.initRoot<OnDisk::StringIndex>();
    auto stringListBuilder = stringIndexBuilder.initStrings(index._strMap.size());

    const int indexFD = FileUtils::openForWrite(_indexPath);
    if (indexFD < 0) {
        logt::CanNotWrite(_indexPath.string());
        return false;
    }

    // Looping through all strings in the StringIndex
    for (const auto& [rstr, sstr] : index._strMap) {
        stringListBuilder[sstr->getID()].setId(sstr->getID());
        stringListBuilder[sstr->getID()].setStr(rstr);
    }

    ::capnp::writeMessageToFd(indexFD, message);
    close(indexFD);

    return true;
}

}
