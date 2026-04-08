#pragma once

namespace db {

class Column;

class ApplyMask {
public:
    static void eval(Column* res, const Column* arg, const Column* mask);
};

}
