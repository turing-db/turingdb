#pragma once

namespace db {

class DBManager;

class InterpreterContext {
public:
    explicit InterpreterContext(DBManager* dbMan)
        : _dbMan(dbMan)
    {
    }

    DBManager* getDBManager() const { return _dbMan; }

private:
    DBManager* _dbMan {nullptr};
};

}
