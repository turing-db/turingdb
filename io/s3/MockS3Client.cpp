#include "MockS3Client.h"

using namespace S3;

MockS3Client::MockS3Client(const MockUploadResult& uploadResult,
                           const MockDownloadResult& downloadResult,
                           const MockListResult& listResult)
    : _uploadResult(uploadResult),
      _downloadResult(downloadResult),
      _listResult(listResult)
{
}
