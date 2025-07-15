#pragma once

#include <memory>

#include "Pattern.h"

namespace db {

class MatchStatement {
public:
private:
    std::shared_ptr<Pattern> _pattern;
};

}
