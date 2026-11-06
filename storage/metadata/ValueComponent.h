#pragma once

#include <variant>

#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "metadata/Point.h"

namespace db {

// The component a read takes of a value that has components: d.year, d.hours, p.x
using ValueComponent = std::variant<DateTimePart, DurationPart, PointPart>;

}
