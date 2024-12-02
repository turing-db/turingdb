#include "FileRegion.h"

#include <sys/mman.h>

using namespace fs;

template <IOType IO>
FileRegion<IO>::~FileRegion() {
    ::munmap(_map, _size);
}


template class fs::FileRegion<IOType::R>;
template class fs::FileRegion<IOType::RW>;
