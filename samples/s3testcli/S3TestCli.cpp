#include <stdlib.h>
#include <iostream>
#include <string>
#include <vector>

#include <linenoise.h>
#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "AwsS3ClientWrapper.h"
#include "FileCache.h"
#include "SystemManager.h"
#include "TuringConfig.h"

void splitString(std::string& string, std::vector<std::string>& result) {
    std::istringstream iss(string);
    std::string token;

    while(iss >> token){
        result.push_back(token);
    }
}
int main() {
    linenoiseHistoryLoad("history.txt");

    auto awsClient = S3::AwsS3ClientWrapper<>();
    auto turingS3client = S3::TuringS3Client(std::move(awsClient));
    db::TuringConfig config;
    db::SystemManager sysMan(config);
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

        if (tokens.front() == "exit") {
            break;
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
            spdlog::info("Loading Graph");
            cache.loadGraph(tokens[1]);
        }
        if (tokens.front() == "sg") {
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
            spdlog::info("Loading Data File");
            cache.loadDataFile(tokens[1]);
        }
        if (tokens.front() == "sdf") {
            spdlog::info("Saving data file To S3");
            cache.saveDataFile(tokens[1]);
        }

        if (tokens.front() == "ldd") {
            spdlog::info("Loading Data Directory");
            cache.loadDataDirectory(tokens[1]);
        }
        if (tokens.front() == "sdd") {
            spdlog::info("Saving data directory To S3");
            cache.saveDataDirectory(tokens[1]);
        }
    }

    return 0;
}
