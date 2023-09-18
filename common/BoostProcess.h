#pragma once

#include <memory>

namespace boost::process {
    class child;
    class group;
}

using ProcessChild = std::unique_ptr<boost::process::child>;
using ProcessGroup = std::unique_ptr<boost::process::group>;
