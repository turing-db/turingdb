#pragma once

#include <string>

namespace db::query {

class Frame;

class Interpreter {
public:
    Interpreter();
    ~Interpreter();

    bool execQuery(const std::string& query, Frame* frame);

private:
};

}
