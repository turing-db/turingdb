#pragma once

#include <string>

namespace db {

class Symbol {
public:
    Symbol(const std::string& name, size_t position);
    ~Symbol();

    const std::string& getName() const { return _name; }
    size_t getPosition() const { return _position; }

private:
    const std::string _name;
    size_t _position {0};
};

}
