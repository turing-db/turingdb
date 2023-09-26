#include "Symbol.h"

using namespace db;

Symbol::Symbol(const std::string& name, size_t position)
    : _name(name), _position(position)
{
}

Symbol::~Symbol() {
}
