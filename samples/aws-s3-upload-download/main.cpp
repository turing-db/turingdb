#include <cstdlib>
#include <string>
#include <filesystem>

#include <spdlog/spdlog.h>

#include "TuringS3Client.h"
#include "TuringS3ClientConfig.h"

using namespace db;

int main(int argc, const char** argv) {
    const std::string localFile = "/home/dev/perf-test/reactomeDB";
    const std::string localFileCopy = "/home/dev/perf-test/reactomeDB_Copy";
    const std::string remoteObject = "reactomeDB-upload";
    const std::string bucket = "alessandro";
    const std::string endpointUrl = "s3.fr-par.scw.cloud";

    TuringS3ClientConfig turingConfig {endpointUrl};
    TuringS3Client turingClient {turingConfig};

    if (!turingClient.uploadFile(bucket, localFile, remoteObject)) {
        spdlog::info("[{}] Upload failed.", SAMPLE_NAME);
        return EXIT_FAILURE;
    }
    spdlog::info("[{}] Successfully uploaded file {} to S3", SAMPLE_NAME, localFile);

    if(!turingClient.downloadFile(bucket, remoteObject, localFileCopy)) {
        spdlog::info("[{}] Download failed.", SAMPLE_NAME);
        return EXIT_FAILURE;
    }
    spdlog::info("[{}] Successfully downloaded {} from S3", SAMPLE_NAME, remoteObject);

    if (!std::filesystem::exists(localFileCopy) || 
        std::filesystem::file_size(localFile) != std::filesystem::file_size(localFileCopy)) {
        spdlog::info("[{}] Upload/download was not consistent.", SAMPLE_NAME);
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
