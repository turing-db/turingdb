#pragma once

#include "PipelineException.h"
#include "S3TransferDirectories.h"
#include "TuringConfig.h"

namespace db {

class S3TransferStepFunctions {
public:
    static std::string getFullLocalPath(const S3TransferDirectory transferDirectory,
                                        const TuringConfig& config,
                                        const std::string& localPath) {
        if (transferDirectory == S3TransferDirectory::DATA) {
            const auto& root = config.getDataDir();
            const auto& fullPath = root / localPath;
            if (!fs::Path::isSubDirectory(root, fullPath)) {
                throw PipelineException(fmt::format("Invalid Suffix Directory {}",
                                                    (fullPath).c_str()));
            }
            return fullPath.c_str();

        } else if (transferDirectory == S3TransferDirectory::GRAPH) {
            const auto& root = config.getGraphsDir();
            const auto& fullPath = root / localPath;

            if (!fs::Path::isSubDirectory(root, fullPath)) {
                throw PipelineException(fmt::format("Invalid Suffix Directory {}",
                                                    (fullPath).c_str()));
            }
            return fullPath.c_str();
        } else {
            throw PipelineException(fmt::format("Invalid S3 Transfer Directory"));
        }
    }
};

}
