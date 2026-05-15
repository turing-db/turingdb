#pragma once

#include <span>

#include "ArcManager.h"

namespace db {

class DataPart;

using DataPartSpan = std::span<const WeakArc<DataPart>>;
using DataPartIterator = DataPartSpan::iterator;

}
