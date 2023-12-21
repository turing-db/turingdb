// Copyright 2023 Turing Biosystems Ltd

#pragma once

#include <memory>

namespace db {

class Storage;

class DB {
public:
    DB();
    ~DB();

private:
    std::unique_ptr<Storage> _storage;
};

}