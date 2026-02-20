#pragma once

#include <functional>
#include <string>
#include <vector>

#include "S3ClientResult.h"

namespace S3 {

struct MockListResult {
    bool _success {true};
    S3ClientErrorType _errorType {S3ClientErrorType::UNKNOWN};
    std::vector<std::string> _keys;
    std::vector<std::string> _commonPrefixes;
};

struct MockUploadResult {
    bool _success {true};
    S3ClientErrorType _errorType {S3ClientErrorType::UNKNOWN};
    int _statusCode {200};
};

struct MockDownloadResult {
    bool _success {true};
    S3ClientErrorType _errorType {S3ClientErrorType::UNKNOWN};
    std::string _content;
};

class MockS3Client {
public:
    MockS3Client() = default;

    MockS3Client(const MockUploadResult& uploadResult,
                 const MockDownloadResult& downloadResult,
                 const MockListResult& listResult);

    void setUploadResult(const MockUploadResult& result) { _uploadResult = result; }
    void setDownloadResult(const MockDownloadResult& result) { _downloadResult = result; }
    void setListResult(const MockListResult& result) { _listResult = result; }

    const MockUploadResult& getUploadResult() const { return _uploadResult; }
    const MockDownloadResult& getDownloadResult() const { return _downloadResult; }
    const MockListResult& getListResult() const { return _listResult; }

private:
    MockUploadResult _uploadResult;
    MockDownloadResult _downloadResult;
    MockListResult _listResult;
};

}
