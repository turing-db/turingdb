#include "DeleteTest.h"

using namespace db;

int main() {
    {
        DeleteTest tester {"simpledb"};
        tester.addQuery("match (n) return n");
        tester.deleteNodes({0,1,2});
        tester.run();
    }

    {
        DeleteTest tester {"simpledb"};
        tester.addQuery("match (n)-[e]-(m) return e");
        tester.deleteEdges({10, 11});
        tester.run();
    }

    return 0;
}
