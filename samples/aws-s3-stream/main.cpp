#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "TuringS3Client.h"
#include "TuringS3ClientConfig.h"
#include "TuringS3OutputStream.h"

using namespace db;

int main(int argc, const char** argv) {
    const std::string localFilePath = "/home/dev/perf-test/100MBFile";
    const std::string remoteObject = "100MBFile-stream-sample";
    const std::string bucket = "alessandro";
    const std::string endpointUrl = "s3.fr-par.scw.cloud";

    TuringS3ClientConfig turingConfig {endpointUrl};
    TuringS3Client turingClient {turingConfig};

    std::ifstream ifile {localFilePath, std::ios::in | std::ios::binary};
    if (auto turingStream = turingClient.createOutputStream(bucket, remoteObject);
        turingStream && ifile.is_open()) {
        *turingStream << ifile.rdbuf();
    } else {
        spdlog::info("[{}] The operation failed.", SAMPLE_NAME);
        return EXIT_FAILURE;
    }
    ifile.close();

    // Use a read operation to verify the file was uploaded
    auto fileSize = std::filesystem::file_size(localFilePath);
    std::vector<std::byte> uploadedData;
    if (!turingClient.readObject(bucket, remoteObject, uploadedData) || fileSize != uploadedData.size()) {
        spdlog::info("[{}] Upload not consistent", SAMPLE_NAME);
        return EXIT_FAILURE;
    }

    spdlog::info("[{}] Successfully streamed {} MB of data.", SAMPLE_NAME, (float)fileSize / 1024.f / 1024.f);
    return EXIT_SUCCESS;
}
