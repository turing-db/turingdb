#pragma once

namespace db {
class DBUniverse;
}

namespace db::query {

class InterpreterContext {
public:
    explicit InterpreterContext(DBUniverse* universe)
        : _universe(universe)
    {
    }

    DBUniverse* getUniverse() const { return _universe; }

private:
    DBUniverse* _universe {nullptr};
};

}
