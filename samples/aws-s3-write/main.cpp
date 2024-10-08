#include <cstdlib>
#include <string>
#include <fstream>

#include <argparse.hpp>
#include <spdlog/spdlog.h>


#include "TuringS3Client.h"
#include "TuringS3ClientConfig.h"
#include "TuringS3OutputStream.h"

using namespace db;

namespace utils {
bool loadFileInMemory(std::string& data, const std::string& path) {
    std::ifstream ifile {path, std::ios::in | std::ios::binary};
    std::ostringstream ss;
    if (ifile.is_open()) {
        ss << ifile.rdbuf();
        data = ss.str();
        ifile.close();
        return true;
    }
    return false;
}

}

int main(int argc, const char** argv) {
    const std::string localFilePath = "/home/dev/perf-test/100MBFile";
    const std::string remoteObject = "100MBFile-write-sample"; 
    const std::string bucket = "alessandro";
    const std::string endpointUrl = "s3.fr-par.scw.cloud";

    TuringS3ClientConfig turingConfig {endpointUrl};
    TuringS3Client turingClient {turingConfig};
    std::string data{};

    if(!utils::loadFileInMemory(data,  localFilePath)) {
        spdlog::info("[{}] Failed to read file: {}", SAMPLE_NAME, localFilePath);
        return EXIT_FAILURE;
    }

    if (!turingClient.writeObject(bucket, data.data(), data.size(), remoteObject)) {
        spdlog::info("[{}] Failed to upload data.", SAMPLE_NAME);
        return EXIT_FAILURE;
    }
    
    // Use a read operation to verify the file was uploaded
    std::vector<std::byte> uploadedData;
    if (!turingClient.readObject(bucket, remoteObject, uploadedData) || data.size() != uploadedData.size()) {
        spdlog::info("[{}] Upload not consistent", SAMPLE_NAME);
        return EXIT_FAILURE;
    }

    spdlog::info("[{}] Successfully written {} MB of data.", SAMPLE_NAME, (float) data.size() / 1024.f / 1024.f);

    return EXIT_SUCCESS;
}
