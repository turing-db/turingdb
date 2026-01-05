#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class S3ConnectNode : public PlanGraphNode {
public:
    explicit S3ConnectNode(std::string_view accessId, std::string_view secretKey, std::string_view region)
        : PlanGraphNode(PlanGraphOpcode::S3_CONNECT),
        _accessId(accessId),
        _secretKey(secretKey),
        _region(region)
    {
    }

    std::string_view getAccessId() const { return _accessId; }
    std::string_view getSecretKey() const { return _secretKey; }
    std::string_view getRegion() const { return _region; }

private:
    std::string_view _accessId;
    std::string_view _secretKey;
    std::string_view _region;
};

}
