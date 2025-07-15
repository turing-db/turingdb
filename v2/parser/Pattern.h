#pragma once

#include <string>
#include <vector>

namespace db {

class PatternElem {
public:
private:
    std::string _name;
};

class PatternPart {
public:
private:
    std::vector<PatternElem> _elems;
};

class Pattern {
public:
private:
    std::vector<PatternPart> _parts;
};

}
