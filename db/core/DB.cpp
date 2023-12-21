// Copyright 2023 Turing Biosystems Ltd

#include "DB.h"

#include "Storage.h"

using namespace db;

DB::DB()
    : _storage(std::make_unique<Storage>())
{
}

DB::~DB() {
}