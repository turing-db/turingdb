#pragma once

class JobSystem;

namespace db {

class DB;

}

class TestUtils {
public:
    static void generateMillionTestDB(db::DB& db, JobSystem& jobSystem);
};
