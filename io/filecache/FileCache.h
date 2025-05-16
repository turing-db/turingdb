#pragma once

#include <string_view>

#include "FileCacheResult.h"
#include "Path.h"
#include "TuringS3Client.h"

namespace db {

class Graph;

template <typename ClientType>
class FileCache {
public:
    FileCache(fs::Path& graphDir, fs::Path& dataDir, ClientType& clientWrapper);

    Result<void> saveGraph(std::string_view graphName);
    Result<void> loadGraph(std::string_view graphName);

    Result<void> listGraphs(std::vector<std::string>& graphs);
    Result<void> listLocalGraphs(std::vector<fs::Path>& graphs);
    // bool loadGraph(const std::string& graphName);
    Result<void> getCommit(Graph* graph, std::string_view graphName);

    Result<void> listData(std::vector<std::string>& files,
                          std::vector<std::string>& folders,
                          std::string_view dir);
    Result<void> listData(std::vector<std::string>& files,
                          std::vector<std::string>& folders);
    Result<void> listLocalData(std::vector<fs::Path>& files,
                               std::vector<fs::Path>& folders,
                               std::string_view dir);
    Result<void> listLocalData(std::vector<fs::Path>& files,
                               std::vector<fs::Path>& folders);

    Result<void> saveDataFile(std::string_view filePath);
    Result<void> loadDataFile(std::string_view filePath);

    Result<void> saveDataDirectory(std::string_view directoryPath);
    Result<void> loadDataDirectory(std::string_view directoryPath);


    void listFiles();

private:
    fs::Path _graphsDir;
    fs::Path _dataDir;
    S3::TuringS3Client<ClientType> _s3Client;
    std::string _userId = "testUser1";
    std::string _bucketName = "suhas-disk-test";
};
}
