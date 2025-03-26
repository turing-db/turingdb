#pragma once

#include <span>

#include "ArcManager.h"

namespace db {

class DataPart;

using DataPartSpan = std::span<const WeakArc<const DataPart>>;
using DataPartIterator = DataPartSpan::iterator;

}
