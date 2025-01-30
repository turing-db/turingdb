#pragma once

namespace db {

class TuringDB;

class TuringServer {
public:
    TuringServer(TuringDB* db);
    ~TuringServer();

    void start();

private:
    TuringDB* _db {nullptr};
};

}
