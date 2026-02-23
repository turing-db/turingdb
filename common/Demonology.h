#pragma once

enum class DemonResult {
    Parent,       // Original process: wait for server readiness then exit
    Intermediate, // Session-leader child superseded by second fork: exit immediately
    Daemon,       // Grandchild daemon: continue running
};

class Demonology {
public:
    static DemonResult demonize();
};
