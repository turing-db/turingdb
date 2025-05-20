#pragma once

#include <aws/s3-crt/S3CrtServiceClientModel.h>

namespace Aws {
namespace S3Crt {
namespace Model {
class PutObjectRequest;
class GetObjectRequest;
class ListObjectsV2Request;
}
}
}

namespace S3 {

class MockS3Client {
public:
    MockS3Client(Aws::S3Crt::Model::PutObjectOutcome& putOutcome,
                 Aws::S3Crt::Model::GetObjectOutcome& getOutcome,
                 Aws::S3Crt::Model::ListObjectsV2Outcome& listOutcome);

    Aws::S3Crt::Model::PutObjectOutcome& PutObject(Aws::S3Crt::Model::PutObjectRequest& request);
    Aws::S3Crt::Model::GetObjectOutcome GetObject(Aws::S3Crt::Model::GetObjectRequest& request);
    Aws::S3Crt::Model::ListObjectsV2Outcome& ListObjectsV2(Aws::S3Crt::Model::ListObjectsV2Request& request);

private:
    Aws::S3Crt::Model::PutObjectOutcome& _putOutcome;
    Aws::S3Crt::Model::GetObjectOutcome& _getOutcome;
    Aws::S3Crt::Model::ListObjectsV2Outcome& _listOutcome;
};

}
