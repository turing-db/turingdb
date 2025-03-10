#pragma once

#include <memory>
#include <span>

namespace db {

class DataPart;

using DataPartSpan = std::span<const std::shared_ptr<DataPart>>;
using DataPartIterator = DataPartSpan::iterator;

}
