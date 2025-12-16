#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class S3TransferNode : public PlanGraphNode {
public:
    enum class Direction : uint8_t {
        PULL = 0,
        PUSH
    };

    explicit S3TransferNode(Direction direction,
                            std::string_view s3Bucket,
                            std::string_view s3Prefix,
                            std::string_view s3File,
                            std::string_view localPath)
        : PlanGraphNode(PlanGraphOpcode::S3_TRANSFER),
        _direction(direction),
        _s3Bucket(s3Bucket),
        _s3Prefix(s3Prefix),
        _s3File(s3File),
        _localPath(localPath)
    {
    }

    Direction getDirection() const { return _direction; }
    std::string_view getS3Bucket() const { return _s3Bucket; }
    std::string_view getS3Prefix() const { return _s3Prefix; }
    std::string_view getS3File() const { return _s3File; }
    std::string_view getLocalPath() const { return _localPath; }

private:
    Direction _direction;
    std::string_view _s3Bucket;
    std::string_view _s3Prefix;
    std::string_view _s3File;
    std::string_view _localPath;
};

}
