#pragma once

#include <memory>
#include <span>

namespace db {

class DataPart;

using DataPartSpan = std::span<const std::unique_ptr<DataPart>>;
using DataPartIterator = DataPartSpan::iterator;

}
