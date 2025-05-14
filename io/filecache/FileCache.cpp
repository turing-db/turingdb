#include "FileCache.h"
#include "AwsS3ClientWrapper.h"

#include <spdlog/spdlog.h>

namespace db {

template <typename ClientType>
FileCache<ClientType>::FileCache(fs::Path& graphDir, fs::Path& dataDir, ClientType& clientWrapper)
	:_graphsDir(graphDir),
	_dataDir(dataDir),
	_s3Client(clientWrapper)
{
}

template <typename ClientType>
void FileCache<ClientType>::listAvailableGraphs(std::vector<fs::Path>& graphs) {
    const auto list = fs::Path(_graphsDir).listDir();
    if (!list) {
        spdlog::error("Failed to list available graphs in {}", _graphsDir.get());
        return;
    }

    for (const auto& path : list.value()) {
        graphs.push_back(path);
    }
}

template <typename ClientType>
void FileCache<ClientType>::listGraphs(std::vector<std::string>& graphs) {
    auto prefix = fmt::format("{}/graphs/", _userId);
    _s3Client.listFolders(_bucketName, graphs, prefix);
}

template <typename ClientType>
bool FileCache<ClientType>::loadGraph(std::string_view graphName) {
    const bool exists = (_graphsDir / graphName).exists();
    if (exists) {
        spdlog::info("{} is present locally", graphName);
        return true;
    }

    std::string graphDirectory = fmt::format("{}/{}/", _graphsDir.c_str(), graphName);
	std::string s3Prefix = fmt::format("{}/graphs/{}/", _userId, graphName);
	spdlog::info(s3Prefix);
	if(auto res = _s3Client.downloadDirectory(graphDirectory, _bucketName, s3Prefix); !res){
		switch (res.error().getType()) {
			case S3::ErrorType::INVALID_DIRECTORY_NAME:
				spdlog::error("Could not find graph in S3");
				break;
			default:
				break;

		}
		return false;
	}

	spdlog::info("Loaded {} from S3 Storage", graphName);
    return true;
}

template <typename ClientType>
bool FileCache<ClientType>::saveGraph(std::string_view graphName) {
    const bool exists = (_graphsDir / graphName).exists();
    if (!exists) {
        spdlog::error("Could not find the graph in the local file cache");
        return false;
    }

    std::string graphDirectory = fmt::format("{}/{}/", _graphsDir.c_str(), graphName);
	std::string s3Prefix = fmt::format("{}/graphs/{}", _userId, graphName);
	if(auto res = _s3Client.uploadDirectory(graphDirectory, _bucketName, s3Prefix);!res){
		spdlog::error(res.error().fmtMessage());
		return false;
	}

	spdlog::info("Saved graph to S3");
    return true;
}

template <typename ClientType>
bool FileCache<ClientType>::saveDataFile(std::string& filePath) {
    const auto fileInfo = (_dataDir / filePath).getFileInfo();
    if (!fileInfo.has_value()) {
        spdlog::error("Could not find {} in {}", filePath, _dataDir.get());
        return false;
    }
    if(fileInfo->_type != fs::FileType::File){
		spdlog::error("{} is not a file", filePath);
		return false;
	}

	std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), filePath);
	std::string s3Prefix = fmt::format("{}/data/{}", _userId, filePath);
	if(auto res = _s3Client.uploadFile(fullLocalPath, _bucketName, s3Prefix);!res){
		spdlog::error(res.error().fmtMessage());
		return false;
	}

	spdlog::info("Saved file: {}", filePath);
    return true;
}

template <typename ClientType>
bool FileCache<ClientType>::loadDataFile(std::string& filePath) {
    const auto info = (_dataDir / filePath).getFileInfo();
    if (info.has_value()) {
        if (info->_type != fs::FileType::File) {
            spdlog::error("{} is not a file", filePath);
			return false;
        }
        spdlog::error("{} present locally", filePath, _graphsDir.get());
		return true;
    }

    std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), filePath);
	std::string s3prefix = _userId + "/data/" + filePath;
	if(auto res = _s3Client.downloadFile(fullLocalPath,_bucketName,s3prefix); !res){
		switch (res.error().getType()) {
			case S3::ErrorType::INVALID_BUCKET_NAME :
				spdlog::error("Bucket Name is invalid");
				break;
			case S3::ErrorType::INVALID_KEY_NAME :
				spdlog::error("Could not find file in s3: {}", filePath);
				break;
			default:
				break;
		}

		spdlog::error("Could not download file");
		return false;
	}

	spdlog::info("Downloaded file: {}", filePath);
    return true;
}

template <typename ClientType>
bool FileCache<ClientType>::saveDataDirectory(std::string& directoryPath) {
    const auto info = (_dataDir / directoryPath).getFileInfo();
    if (!info.has_value()) {
        spdlog::error("Could not find {} in {}", directoryPath, _dataDir.get());
        return false;
    }

    if(info->_type != fs::FileType::Directory){
		spdlog::error("{} is not a directory", directoryPath);
		return false;
	}

	std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), directoryPath);
	std::string dataDirPrefix = fmt::format("{}/data/{}", _userId, directoryPath);

	if(auto res = _s3Client.uploadDirectory(fullLocalPath, _bucketName, dataDirPrefix);!res){
		spdlog::error(res.error().fmtMessage());
		return false;
	}

	spdlog::info("Saved Directory: {}", directoryPath);
    return true;
}

template <typename ClientType>
bool FileCache<ClientType>::loadDataDirectory(std::string& directoryPath) {
    const auto info = (_dataDir / directoryPath).getFileInfo();

    if (info.has_value()) {
        if(info->_type != fs::FileType::Directory){
			spdlog::error("{} is not a directory", directoryPath);
			return false;
		}
		spdlog::error("{} present locally", directoryPath, _graphsDir.get());
		return true;
    }

    std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), directoryPath);
	std::string prefix = _userId + "/data/" + directoryPath;
	if(auto res = _s3Client.downloadDirectory(fullLocalPath,_bucketName,prefix); !res){
		spdlog::error("Could not download directory");
		return false;
	}

	spdlog::info("Downloaded directory: {}", directoryPath);
    return true;
}

template class FileCache<S3::AwsS3ClientWrapper<>>;
}
