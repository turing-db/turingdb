#include <cstdlib>
#include <string>
#include <vector>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "TuringS3Client.h"
#include "TuringS3ClientConfig.h"
#include "TuringS3OutputStream.h"

using namespace db;

int main(int argc, const char** argv) {
    const std::string remoteObject = "db/reactome/db/data";
    const std::string bucket = "turing";
    const std::string endpointUrl = "s3.fr-par.scw.cloud";

    TuringS3ClientConfig turingConfig {endpointUrl};
    TuringS3Client turingClient {turingConfig};
    std::vector<std::byte> data;

    if (!turingClient.readObject(bucket, remoteObject, data)) {
        spdlog::info("[{}] The operation failed.", SAMPLE_NAME);
        return EXIT_FAILURE;
    }

    if (data.size() == 0) {
        spdlog::info("[{}] No data retrieved.", SAMPLE_NAME);
        return EXIT_FAILURE;
    }

    spdlog::info("[{}] Successfully read {} MB of data.", SAMPLE_NAME, (float)data.size() / 1024.f / 1024.f);
    return EXIT_SUCCESS;
}
