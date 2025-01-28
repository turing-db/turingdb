#include "FileRegion.h"

#include <sys/mman.h>

using namespace fs;

FileRegion::~FileRegion() {
    ::munmap(_map, _size);
}
