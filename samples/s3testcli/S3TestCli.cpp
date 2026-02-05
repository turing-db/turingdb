#include <stdlib.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <linenoise.h>
#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "MinioS3ClientWrapper.h"
#include "FileCache.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"

void splitString(std::string& string, std::vector<std::string>& result) {
    std::istringstream iss(string);
    std::string token;

    while(iss >> token){
        result.push_back(token);
    }
}

struct TreeNode {
    std::map<std::string, TreeNode> children;
    bool isFile = false;
};

void buildTree(TreeNode& root, const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
        TreeNode* current = &root;
        std::istringstream iss(key);
        std::string part;

        while (std::getline(iss, part, '/')) {
            if (part.empty()) {
                continue;
            }
            current = &current->children[part];
        }
        if (!key.empty() && key.back() != '/') {
            current->isFile = true;
        }
    }
}

void printTree(const TreeNode& node, const std::string& prefix, bool isLast) {
    auto it = node.children.begin();
    size_t count = 0;
    const size_t total = node.children.size();

    for (; it != node.children.end(); ++it, ++count) {
        const bool lastChild = (count == total - 1);
        const std::string connector = lastChild ? "└── " : "├── ";
        const std::string extension = lastChild ? "    " : "│   ";

        std::cout << prefix << connector << it->first;
        if (!it->second.isFile && !it->second.children.empty()) {
            std::cout << "/";
        }
        std::cout << std::endl;

        if (!it->second.children.empty()) {
            printTree(it->second, prefix + extension, lastChild);
        }
    }
}
int main() {
    linenoiseHistoryLoad("history.txt");

    auto minioClient = S3::MinioS3ClientWrapper();
    auto turingS3client = S3::TuringS3Client(std::move(minioClient));
    db::TuringConfig config;
    config.setSyncedOnDisk(false);
    db::TuringDB db(&config);
    db.init();

    const auto& turingDir = config.getTuringDir();

    db::FileCache cache = db::FileCache(turingDir / "graphs", turingDir / "data", turingS3client);
    // Main input loop
    char* line = nullptr;
    while ((line = linenoise("prompt> ")) != nullptr) {
        std::vector<std::string> tokens;
        // Add to history
        linenoiseHistoryAdd(line);
        linenoiseHistorySave("history.txt");

        // Process the line
        std::string input(line);
        std::cout << "You entered:" << input << std::endl;

        splitString(input, tokens);

        // Free memory
        free(line);

        if (tokens.empty()) {
            continue;
        }

        if (tokens.front() == "exit") {
            break;
        }

        if (tokens.front() == "inits3") {
            spdlog::info("Initializing S3 storage");
            auto result = cache.initS3Storage();
            if (!result) {
                spdlog::error("Failed to initialize S3 storage");
            }
        }

        if (tokens.front() == "bucket") {
            if (tokens.size() < 2) {
                std::cout << "Current bucket: " << cache.getBucketName() << std::endl;
            } else {
                cache.setBucketName(tokens[1]);
                spdlog::info("Bucket set to: {}", tokens[1]);
            }
        }

        if (tokens.front() == "ls") {
            spdlog::info("Listing bucket contents as tree");
            std::vector<std::string> keys;
            auto result = cache.getS3Client().listKeys(cache.getBucketName(), "", keys);
            if (!result) {
                spdlog::error("Failed to list bucket contents: {}",
                              result.error().fmtMessage());
            } else {
                std::cout << cache.getBucketName() << std::endl;
                TreeNode root;
                buildTree(root, keys);
                printTree(root, "", true);
            }
        }

        if (tokens.front() == "lslg") {
            spdlog::info("Listing local graphs");
            std::vector<fs::Path> graphs;
            cache.listLocalGraphs(graphs);

            for (const auto& graph : graphs) {
                std::cout << graph.c_str() << std::endl;
            }
        }

        if (tokens.front() == "lsg") {
            spdlog::info("Listing S3 stored graphs");
            std::vector<std::string> graphs;
            cache.listGraphs(graphs);

            for(const auto& graph: graphs){
                std::cout<<graph<<std::endl;
            }

        }

        if (tokens.front() == "lg") {
            if (tokens.size() < 2) {
                spdlog::error("Usage: lg <graph_name>");
                continue;
            }
            spdlog::info("Loading Graph");
            cache.loadGraph(tokens[1]);
        }
        if (tokens.front() == "sg") {
            if (tokens.size() < 2) {
                spdlog::error("Usage: sg <graph_name>");
                continue;
            }
            spdlog::info("Saving Graph To S3");
            cache.saveGraph(tokens[1]);
        }

        if (tokens.front() == "lsd") {
            spdlog::info("Listing data");
            std::vector<std::string> files;
            std::vector<std::string> folders;
            if (tokens.size() == 2) {
                cache.listData(files, folders, tokens[1]);
            } else {
                cache.listData(files, folders);
            }

            std::cout << "Files:" << std::endl;
            for (const auto& file : files) {
                std::cout << file.c_str() << std::endl;
            }
            std::cout << "Folders:" << std::endl;
            for (const auto& folder : folders) {
                std::cout << folder.c_str() << std::endl;
            }
        }

        if (tokens.front() == "lsld") {
            spdlog::info("Listing local data");
            std::vector<fs::Path> folders;
            std::vector<fs::Path> files;
            if (tokens.size() == 2) {
                cache.listLocalData(files, folders, tokens[1]);
            } else {
                cache.listLocalData(files, folders);
            }

            std::cout << "Files:" << std::endl;
            for (const auto& file : files) {
                std::cout << file.c_str() << std::endl;
            }
            std::cout << "Folders:" << std::endl;
            for (const auto& folder : folders) {
                std::cout << folder.c_str() << std::endl;
            }
        }

        if (tokens.front() == "ldf") {
            if (tokens.size() < 2) {
                spdlog::error("Usage: ldf <file_path>");
                continue;
            }
            spdlog::info("Loading Data File");
            cache.loadDataFile(tokens[1]);
        }
        if (tokens.front() == "sdf") {
            if (tokens.size() < 2) {
                spdlog::error("Usage: sdf <file_path>");
                continue;
            }
            spdlog::info("Saving data file To S3");
            cache.saveDataFile(tokens[1]);
        }

        if (tokens.front() == "ldd") {
            if (tokens.size() < 2) {
                spdlog::error("Usage: ldd <directory_path>");
                continue;
            }
            spdlog::info("Loading Data Directory");
            cache.loadDataDirectory(tokens[1]);
        }
        if (tokens.front() == "sdd") {
            if (tokens.size() < 2) {
                spdlog::error("Usage: sdd <directory_path>");
                continue;
            }
            spdlog::info("Saving data directory To S3");
            cache.saveDataDirectory(tokens[1]);
        }
    }

    return 0;
}
