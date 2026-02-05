#pragma once

#include <string_view>

#include "FileCacheResult.h"
#include "TuringS3Client.h"
#include "Path.h"

namespace db {

class Graph;

template <typename ClientType>
class FileCache {
public:
    FileCache(const fs::Path& graphDir,
              const fs::Path& dataDir,
              S3::TuringS3Client<ClientType>& clientWrapper);

    FileCacheResult<void> saveGraph(std::string_view graphName);
    FileCacheResult<void> loadGraph(std::string_view graphName);

    FileCacheResult<void> listGraphs(std::vector<std::string>& graphs);
    FileCacheResult<void> listLocalGraphs(std::vector<fs::Path>& graphs);

    FileCacheResult<void> listData(std::vector<std::string>& files,
                                   std::vector<std::string>& folders,
                                   std::string_view dir);
    FileCacheResult<void> listData(std::vector<std::string>& files,
                                   std::vector<std::string>& folders);
    FileCacheResult<void> listLocalData(std::vector<fs::Path>& files,
                                        std::vector<fs::Path>& folders,
                                        std::string_view dir);
    FileCacheResult<void> listLocalData(std::vector<fs::Path>& files,
                                        std::vector<fs::Path>& folders);

    FileCacheResult<void> saveDataFile(std::string_view filePath);
    FileCacheResult<void> loadDataFile(std::string_view filePath);

    FileCacheResult<void> saveDataDirectory(std::string_view directoryPath);
    FileCacheResult<void> loadDataDirectory(std::string_view directoryPath);

    FileCacheResult<void> initS3Storage();

    void setBucketName(const std::string& bucketName);
    const std::string& getBucketName() const;

    S3::TuringS3Client<ClientType>& getS3Client();

private:
    fs::Path _graphsDir;
    fs::Path _dataDir;
    S3::TuringS3Client<ClientType>& _s3Client;
    std::string _userId = "turing";
    std::string _bucketName = "turing-disk-test";
};
}
