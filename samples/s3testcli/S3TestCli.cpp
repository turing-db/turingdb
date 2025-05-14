#include <stdlib.h>
#include <argparse.hpp>
#include <iostream>
#include <linenoise.h>
#include <string>
#include <vector>
#include <istream>

#include "AwsS3ClientWrapper.h"
#include "FileCache.h"
#include "SystemManager.h"
#include "spdlog/spdlog.h"


void splitString(std::string& string, std::vector<std::string>& result){
    std::istringstream iss(string);
    std::string token;

    while(iss >> token){
        result.push_back(token);
    }
}
int main() {
    linenoiseHistoryLoad("history.txt");

    auto awsClient = S3::AwsS3ClientWrapper<>();
    db::SystemManager sysMan;
    db::FileCache cache = db::FileCache(sysMan.getGraphsDir(), sysMan.getDataDir(), awsClient);
    // Main input loop
    char* line = nullptr;
    while ((line = linenoise("prompt> ")) != nullptr) {
        std::vector<std::string> tokens;
        // Add to history
        linenoiseHistoryAdd(line);
        linenoiseHistorySave("history.txt");

        // Process the line
        std::string input(line);
        std::cout << "You entered: " << input << std::endl;

        splitString(input, tokens);

        // Free memory
        free(line);

        if (tokens.front() == "exit") { break;
        }

        if (tokens.front() == "lb") {
            // client.listBuckets();
        }

        if (tokens.front() == "lo") {
            std::vector<std::string> results;
            std::cout << tokens.size() << std::endl;
            if (tokens.size() == 3) {
                // client.listFiles(tokens[1], results,tokens[2]);
            } else {
                std::string empty;
                // client.listFiles(tokens[1], results,empty);
            }
            for (const auto& result : results) {
                std::cout << result << std::endl;
            }
        }

        if (tokens.front() == "lf") {
            std::vector<std::string> results;
            std::cout << tokens.size() << std::endl;
            if (tokens.size() == 3) {
                // client.listFolders(tokens[1], results,tokens[2]);
            } else {
                std::string empty;
                // client.listFolders(tokens[1], results,empty);
            }
            for (const auto& result : results) {
                std::cout << result << std::endl;
            }
        }

        if (tokens.front() == "cb") {
            // client.createBucket(tokens[1]);
        }

        if (tokens.front() == "db") {
            // client.deleteBucket(tokens[1]);
        }

        if (tokens.front() == "uf") {
            std::cout << "Uploading File: " << tokens[1] << std::endl;
            // client.uploadFile(tokens[1],tokens[2],tokens[3]);
        }
        if (tokens.front() == "df") {
            // client.downloadFile(tokens[1],tokens[2],tokens[3]);
        }

        if (tokens.front() == "ud") {
            std::cout << "Uploading directory: " << tokens[1] << std::endl;
            // client.uploadDirectory(tokens[1],tokens[2],tokens[3]);
        }

        if (tokens.front() == "dd") {
            std::cout << "Downloading directory: " << tokens[1] << std::endl;
            // client.downloadDirectory(tokens[1],tokens[2],tokens[3]);
        }

        if (tokens.front() == "lsag") {
            spdlog::info("Listing local graphs");
            std::vector<fs::Path> graphs;
            cache.listAvailableGraphs(graphs);

            for(const auto& graph: graphs){
                std::cout<<graph.c_str()<<std::endl;
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
