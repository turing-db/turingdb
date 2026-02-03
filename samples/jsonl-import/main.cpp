#include <memory>
#include <sstream>

#include "Graph.h"
#include "JobSystem.h"
#include "JsonlParser.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/Transaction.h"

using namespace db;

int main(int argc, const char** argv) {
    std::stringstream input {R"(
        {"type":"node","id":"0","labels":["Founder","Person","SoftwareEngineering"],"properties":{"isFrench":true,"dob":"18/01","hasPhD":true,"name":"Remy","age":32}}
        {"type":"node","id":"1","labels":["Bioinformatics","Founder","Person"],"properties":{"isFrench":true,"dob":"18/08","hasPhD":true,"name":"Adam","age":"32"}}
        {"type":"node","id":"2","labels":["Person","SoftwareEngineering"],"properties":{"isFrench":true,"dob":"28/05","hasPhD":true,"name":"Luc"}}
        {"type":"node","id":"3","labels":["Bioinformatics","Person"],"properties":{"isFrench":true,"dob":"24/07","hasPhD":false,"name":"Maxime"}}
        {"type":"node","id":"4","labels":["Bioinformatics","Person"],"properties":{"isFrench":false,"hasPhD":true,"name":"Martina"}}
        {"type":"node","id":"5","labels":["Person","SoftwareEngineering"],"properties":{"isFrench":false,"hasPhD":false,"name":"Suhas"}}
        {"type":"node","id":"6","labels":["Exotic","Interest","SleepDisturber","Supernatural"],"properties":{"isReal":true,"name":"Ghosts"}}
        {"type":"node","id":"7","labels":["Interest"],"properties":{"name":"Bio"}}
        {"type":"node","id":"8","labels":["Interest"],"properties":{"name":"Cooking"}}
        {"type":"node","id":"9","labels":["Interest"],"properties":{"name":"Paddle"}}
        {"type":"node","id":"10","labels":["Interest","SleepDisturber"],"properties":{"isReal":true,"name":"Animals"}}
        {"type":"node","id":"11","labels":["Interest"],"properties":{"isReal":true,"name":"Computers"}}
        {"type":"node","id":"12","labels":["Exotic","Interest"],"properties":{"isReal":false,"name":"Eighties"}}
        {"type":"relationship","id":"0","label":"KNOWS_WELL","properties":{"duration":20,"name":"Remy -> Adam"},"start":{"id":"0","labels":["Founder","Person","SoftwareEngineering"]," properties":{"isFrench":true,"dob":"18/01","hasPhD":true,"name":"Remy","age":32}},"end":{"id":"1","labels":["Bioinformatics","Founder","Person"],"properties":{"isFrench":true, "dob":"18/08","hasPhD":true,"name":"Adam","age":32}}}
        {"type":"relationship","id":"1","label":"KNOWS_WELL","properties":{"duration":20,"name":"Adam -> Remy"},"start":{"id":"1","labels":["Bioinformatics","Founder","Person"],"prope rties":{"isFrench":true,"dob":"18/08","hasPhD":true,"name":"Adam","age":32}},"end":{"id":"0","labels":["Founder","Person","SoftwareEngineering"],"properties":{"isFrench":true, "dob":"18/01","hasPhD":true,"name":"Remy","age":32}}}
        {"type":"relationship","id":"2","label":"INTERESTED_IN","properties":{"duration":20,"name":"Remy -> Ghosts","proficiency":"expert"},"start":{"id":"0","labels":["Founder","Pers on","SoftwareEngineering"],"properties":{"isFrench":true,"dob":"18/01","hasPhD":true,"name":"Remy","age":32}},"end":{"id":"6","labels":["Exotic","Interest","SleepDisturber","S upernatural"],"properties":{"isReal":true,"name":"Ghosts"}}}
        {"type":"relationship","id":"3","label":"INTERESTED_IN","properties":{"name":"Remy -> Computers","proficiency":"expert"},"start":{"id":"0","labels":["Founder","Person","Softwa reEngineering"],"properties":{"isFrench":true,"dob":"18/01","hasPhD":true,"name":"Remy","age":32}},"end":{"id":"11","labels":["Interest"],"properties":{"isReal":true,"name":"C omputers"}}}
        {"type":"relationship","id":"4","label":"INTERESTED_IN","properties":{"duration":20,"name":"Remy -> Eighties","proficiency":"moderate"},"start":{"id":"0","labels":["Founder"," Person","SoftwareEngineering"],"properties":{"isFrench":true,"dob":"18/01","hasPhD":true,"name":"Remy","age":32}},"end":{"id":"12","labels":["Exotic","Interest"],"properties": {"isReal":false,"name":"Eighties"}}}
        {"type":"relationship","id":"5","label":"INTERESTED_IN","properties":{"name":"Adam -> Bio"},"start":{"id":"1","labels":["Bioinformatics","Founder","Person"],"properties":{"isF rench":true,"dob":"18/08","hasPhD":true,"name":"Adam","age":32}},"end": "7"}
        {"type":"relationship","id":"6","label":"INTERESTED_IN","properties":{"name":"Adam -> Cooking"},"start":{"id":"1","labels":["Bioinformatics","Founder","Person"],"properties":{ "isFrench":true,"dob":"18/08","hasPhD":true,"name":"Adam","age":32}},"end": 8}
        {"type":"relationship","id":"7","label":"KNOWS_WELL","properties":{"duration":200,"name":"Ghosts -> Remy","proficiency":"expert"},"start":{"id":"6","labels":["Exotic","Interes t","SleepDisturber","Supernatural"],"properties":{"isReal":true,"name":"Ghosts"}},"end":{"id":"0","labels":["Founder","Person","SoftwareEngineering"],"properties":{"isFrench": true,"dob":"18/01","hasPhD":true,"name":"Remy","age":32}}}
        {"type":"relationship","id":"8","label":"INTERESTED_IN","properties":{"duration":20,"name":"Luc -> Animals"},"start":{"id":"2","labels":["Person","SoftwareEngineering"],"prope rties":{"isFrench":true,"dob":"28/05","hasPhD":true,"name":"Luc"}},"end":{"id":"10","labels":["Interest","SleepDisturber"],"properties":{"isReal":true,"name":"Animals"}}}
        {"type":"relationship","id":"9","label":"INTERESTED_IN","properties":{"duration":15,"name":"Luc -> Computers"},"start":{"id":"2","labels":["Person","SoftwareEngineering"],"pro perties":{"isFrench":true,"dob":"28/05","hasPhD":true,"name":"Luc"}},"end":{"id":"11","labels":["Interest"],"properties":{"isReal":true,"name":"Computers"}}}
        {"type":"relationship","id":"10","label":"INTERESTED_IN","properties":{"name":"Maxime -> Bio"},"start":{"id":"3","labels":["Bioinformatics","Person"],"properties":{"isFrench": true,"dob":"24/07","hasPhD":false,"name":"Maxime"}},"end":{"id":"7","labels":["Interest"],"properties":{"name":"Bio"}}}
        {"type":"relationship","id":"11","label":"INTERESTED_IN","properties":{"name":"Maxime -> Paddle","proficiency":"expert"},"start":{"id":"3","labels":["Bioinformatics","Person"] ,"properties":{"isFrench":true,"dob":"24/07","hasPhD":false,"name":"Maxime"}},"end":{"id":"9","labels":["Interest"],"properties":{"name":"Paddle"}}}
        {"type":"relationship","id":"12","label":"INTERESTED_IN","properties":{"duration":10,"name":"Martina -> Cooking"},"start":{"id":"4","labels":["Bioinformatics","Person"],"prope rties":{"isFrench":false,"hasPhD":true,"name":"Martina"}},"end":{"id":"8","labels":["Interest"],"properties":{"name":"Cooking"}}}
    )"};

    std::unique_ptr<JobSystem> js = JobSystem::create();

    const fs::Path turingDir {SAMPLE_DIR "/.turing"};
    if (turingDir.exists()) {
        fmt::println("Turing directory already exists: {}", turingDir.get());
        fmt::println("Removing it...");

        if (const auto res = turingDir.rm(); !res) {
            fmt::println("Could not remove turing directory: {}", res.error().fmtMessage());
            return 1;
        }
    }

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    TuringDB db {&config};

    SystemManager& sysMan = db.getSystemManager();
    Graph* graph = sysMan.createGraph("test");

    std::unique_ptr<Change> change = graph->newChange();
    ChangeAccessor accessor(change->access());

    {
        const auto res = JsonlParser::parse(accessor, input);
        if (!res) {
            fmt::println("{}", res.error().fmtMessage());
            return 1;
        }
    }

    {
        const auto res = accessor.submit(*js);
        if (!res) {
            fmt::println("{}", res.error().fmtMessage());
            return 1;
        }
    }

    fmt::println("Submitted change");
    auto tx = graph->openTransaction();
    const GraphReader reader = tx.readGraph();
    fmt::println("Graph has {} nodes and {} edges", reader.getNodeCount(), reader.getEdgeCount());

    sysMan.dumpGraph("test");

    return 0;
}
