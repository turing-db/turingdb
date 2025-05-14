#pragma once

#include <string_view>

#include "Path.h"
#include "TuringS3Client.h"

namespace db {

class Graph;

template <typename ClientType>
class FileCache {
public:
    FileCache(fs::Path& graphDir, fs::Path& dataDir, ClientType& clientWrapper);

    bool saveGraph(std::string_view graphName);
    bool loadGraph(std::string_view graphName);

    void listGraphs(std::vector<std::string>& graphs);
	void listAvailableGraphs(std::vector<fs::Path>& graphs);
	//bool loadGraph(const std::string& graphName);
	bool getCommit(Graph *graph, std::string_view graphName);


	bool saveDataFile(std::string& filePath);
	bool loadDataFile(std::string& filePath);

	bool saveDataDirectory(std::string& directoryPath);
	bool loadDataDirectory(std::string& directoryPath);


	void listFiles();

private:
    fs::Path _graphsDir;
    fs::Path _dataDir;
    S3::TuringS3Client<ClientType> _s3Client;
    std::string _userId = "testUser1";
    std::string _bucketName = "suhas-disk-test";
};
}
