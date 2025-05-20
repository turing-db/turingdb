#include "MockS3Client.h"

#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>

using namespace S3;

MockS3Client::MockS3Client(Aws::S3Crt::Model::PutObjectOutcome& putOutcome,
                           Aws::S3Crt::Model::GetObjectOutcome& getOutcome,
                           Aws::S3Crt::Model::ListObjectsV2Outcome& listOutcome)
    :_putOutcome(putOutcome),
    _getOutcome(getOutcome),
    _listOutcome(listOutcome)
{
}

Aws::S3Crt::Model::PutObjectOutcome& MockS3Client::PutObject(Aws::S3Crt::Model::PutObjectRequest& request) {
    return _putOutcome;
}

Aws::S3Crt::Model::GetObjectOutcome MockS3Client::GetObject(Aws::S3Crt::Model::GetObjectRequest& request) {
    return std::move(_getOutcome);
}

Aws::S3Crt::Model::ListObjectsV2Outcome& MockS3Client::ListObjectsV2(Aws::S3Crt::Model::ListObjectsV2Request& request) {
    return _listOutcome;
}
