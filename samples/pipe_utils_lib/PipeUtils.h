#pragma once

#include <string>
#include <memory>

namespace db {
    class SystemManager;
}

class JobSystem;

class PipeSample {
public:
    PipeSample(const std::string& sampleName);
    ~PipeSample();

    std::string getTuringHome() const;

    bool loadJsonDB(const std::string& jsonDir);
    void createSimpleGraph();

    bool executeQuery(const std::string& queryStr);

    void startHttpServer();

private:
    std::string _sampleName;
    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<db::SystemManager> _system;
};