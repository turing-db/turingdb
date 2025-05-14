// #include "AwsS3ClientWrapper.h"
//
// #include <aws/core/Aws.h>
// #include <aws/s3-crt/S3CrtClient.h>
//
//
// #include <fstream>
//
// namespace S3{
//
// void AwsS3ClientWrapper::init() {
//	_options = std::make_unique<Aws::SDKOptions>();
//	_options->loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
//	Aws::InitAPI(*_options);
//
//	_clientConfig = std::make_unique<Aws::S3Crt::ClientConfiguration>();
//	_clientConfig->region = "eu-west-2";
//
//	//All client config options need to changed before here
//	_client = Aws::MakeShared<Aws::S3Crt::S3CrtClient>("S3Client",*_clientConfig);
// }
//
// Aws::S3Crt::Model::PutObjectOutcome AwsS3ClientWrapper::putObject(Aws::S3Crt::Model::PutObjectRequest& request) {
//         return _client->PutObject(request);
//
// }
//
// Aws::S3Crt::Model::GetObjectOutcome AwsS3ClientWrapper::getObject(Aws::S3Crt::Model::GetObjectRequest& request){
// }
//
// }
//
//
//
