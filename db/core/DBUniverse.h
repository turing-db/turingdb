#pragma once

#include <map>
#include <string>
#include <shared_mutex>
#include <vector>

namespace db {

class DB;

class DBUniverse {
public:
    DBUniverse();
    ~DBUniverse();

    void getDatabases(std::vector<std::string>& databases) const;

private:
    mutable std::shared_mutex _lock;
    std::map<std::string, DB*> _databases;
};

}
