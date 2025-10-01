#pragma once

#include <memory>

#include "TuringConfig.h"
#include "TuringDB.h"
#include "LocalMemory.h"
#include "Path.h"

using namespace db;

namespace turing::test {

class TuringTestEnv {
public:
    TuringTestEnv();
    ~TuringTestEnv();

    TuringTestEnv(const TuringTestEnv&) = delete;
    TuringTestEnv(TuringTestEnv&&) = delete;
    TuringTestEnv& operator=(const TuringTestEnv&) = delete;
    TuringTestEnv& operator=(TuringTestEnv&&) = delete;

    [[nodiscard]] static std::unique_ptr<TuringTestEnv> create(const fs::Path& turingDir);
    [[nodiscard]] static std::unique_ptr<TuringTestEnv> createSyncedOnDisk(const fs::Path& turingDir);

    [[nodiscard]] TuringConfig& getConfig() {
        return _config;
    }

    [[nodiscard]] TuringDB& getDB() {
        return _db;
    }

    [[nodiscard]] LocalMemory& getMem() {
        return _mem;
    }

    [[nodiscard]] JobSystem& getJobSystem() {
        return _db.getJobSystem();
    }

    [[nodiscard]] SystemManager& getSystemManager() {
        return _db.getSystemManager();
    }

private:
    TuringConfig _config;
    TuringDB _db {&_config};
    LocalMemory _mem;
};

}
