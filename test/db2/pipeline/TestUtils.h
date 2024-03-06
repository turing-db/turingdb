#pragma once

namespace db {

class DBAccess;

}

class TestUtils {
public:
    static void generateMillionTestDB(db::DBAccess& access);
};
