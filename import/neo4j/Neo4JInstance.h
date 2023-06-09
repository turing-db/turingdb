#pragma once

#include <filesystem>

class Neo4JInstance {
public:
    Neo4JInstance();
    ~Neo4JInstance();

    bool setup();
    bool start();
    bool stop();
    void destroy();
    bool isReady() const;

    bool importDBDir(const std::string& dbPath);
    bool changePassword(const std::string& oldPassword, const std::string& newPassword);
    bool importDumpedDB(const std::filesystem::path& dbFilePath) const;

private:
    const std::filesystem::path _neo4jDir;
    const std::filesystem::path _neo4jArchive;
    const std::filesystem::path _neo4jBinary;
    const std::filesystem::path _neo4jAdminBinary;
};
