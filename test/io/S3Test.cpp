#include "TuringS3Client.h"
#include "TuringTest.h"

#include <fstream>
#include <filesystem>
#include <sys/unistd.h>

#include <aws/s3-crt/model/Object.h>
#include <aws/s3-crt/model/ListObjectsV2Result.h>
#include <aws/s3-crt/model/Error.h>

#include "MockS3Client.h"
#include "AwsS3ClientWrapper.h"
#include "DummyDirectory.h"

using namespace turing::test;

class S3Test : public TuringTest {
protected:
    std::string _tempTestDir = std::filesystem::current_path().string() + "/" + _testName + ".tmp/";
    void initialize() override {
        std::filesystem::create_directory(_tempTestDir);
    }

    void terminate() override {
        std::filesystem::remove_all(_tempTestDir);
    }
};

TEST_F(S3Test, SucessfulListOperations) {
    Aws::S3Crt::Model::ListObjectsV2Result listResult;

    std::vector<std::string> keyNames = {"dir0/dir1/file1", "dir0/file2", "file3"};
    std::vector<std::string> fileNames = {"file1", "file2", "file3"};
    std::vector<std::string> keyResults;
    std::vector<std::string> fileResults;

    Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
    for (size_t i = 0; i < keyNames.size(); i++) {
        objects[i].SetKey(keyNames[i]);
    }

    std::vector<std::string> commonPrefixNames = {"dir0/dir0.5/dir1/", "dir0/dir0.5/dir2/", "dir0/dir0.5/dir3/"};
    std::vector<std::string> folderNames = {"dir1", "dir2", "dir3"};
    std::vector<std::string> folderResults;

    Aws::Vector<Aws::S3Crt::Model::CommonPrefix> commonPrefixes(commonPrefixNames.size());
    for (size_t i = 0; i < commonPrefixNames.size(); i++) {
        commonPrefixes[i].SetPrefix(commonPrefixNames[i]);
    }

    listResult.SetContents(objects);
    listResult.SetCommonPrefixes(commonPrefixes);

    Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
    Aws::S3Crt::Model::PutObjectOutcome putOutcome;
    Aws::S3Crt::Model::GetObjectOutcome getOutcome;

    S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
    S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
    S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

    auto res = turingS3Client.listKeys("bucketName", "prefix/", keyResults);
    ASSERT_TRUE(res);
    EXPECT_EQ(keyNames, keyResults);

    res = turingS3Client.listFiles("bucketName", "prefix/", fileResults);
    ASSERT_TRUE(res);
    EXPECT_EQ(fileNames, fileResults);

    res = turingS3Client.listFolders("bucketName", "prefix/", folderResults);
    ASSERT_TRUE(res);
    EXPECT_EQ(folderNames, folderResults);
}

TEST_F(S3Test, UnsucessfulListOperations) {
    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        std::vector<std::string> keyResults;
        std::vector<std::string> fileResults;
        std::vector<std::string> folderResults;

        auto res = turingS3Client.listKeys("bucketName", "prefix/", keyResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);

        res = turingS3Client.listFiles("bucketName", "prefix/", fileResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);

        res = turingS3Client.listFolders("bucketName", "prefix/", folderResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        std::vector<std::string> keyResults;
        std::vector<std::string> fileResults;
        std::vector<std::string> folderResults;

        auto res = turingS3Client.listKeys("bucketName", "prefix/", keyResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);

        res = turingS3Client.listFiles("bucketName", "prefix/", fileResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);

        res = turingS3Client.listFolders("bucketName", "prefix/", folderResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    {
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        std::vector<std::string> keyResults;
        std::vector<std::string> fileResults;
        std::vector<std::string> folderResults;

        auto res = turingS3Client.listKeys("bucketName", "prefix/", keyResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_LIST_KEYS);

        res = turingS3Client.listFiles("bucketName", "prefix/", fileResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_LIST_FILES);

        res = turingS3Client.listFolders("bucketName", "prefix/", folderResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_LIST_FOLDERS);
    }
}

TEST_F(S3Test, SuccesfulFileUpload) {
    Aws::S3Crt::Model::PutObjectResult putResult;

    Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);
    Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
    Aws::S3Crt::Model::GetObjectOutcome getOutcome;

    S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
    S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
    S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

    auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
    ASSERT_TRUE(res);
}

TEST_F(S3Test, UnsuccesfulFileUpload) {
    {
        Aws::S3Crt::Model::PutObjectResult putResult;

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadFile("/does/not/exist", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::FILE_NOT_FOUND);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::UNKNOWN;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        s3Err.SetResponseCode(Aws::Http::HttpResponseCode::PRECONDITION_FAILED);

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::FILE_EXISTS);
    }

    // Covers Unspecified S3 Error Case
    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::UNKNOWN;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_UPLOAD_FILE);
    }
}

TEST_F(S3Test, SuccesfulFileDownload) {
    // Aws makes an empty stream for the result if none is provided
    Aws::S3Crt::Model::GetObjectResult getResult;

    Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
    Aws::S3Crt::Model::PutObjectOutcome putOutcome;
    Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;

    S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
    S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
    S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

    auto res = turingS3Client.downloadFile("/dev/null", "bucketName", "keyName");
    ASSERT_TRUE(res);
}

TEST_F(S3Test, UnsuccesfulFileDownload) {
    {
        Aws::S3Crt::Model::GetObjectResult getResult;

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadFile("/does/not/exist", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_OPEN_FILE);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadFile(_tempTestDir + "err", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::NO_SUCH_KEY;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadFile(_tempTestDir + "err", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_KEY_NAME);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadFile(_tempTestDir + "err", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    // Covers Unspecified S3 Error Case
    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::UNKNOWN;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadFile(_tempTestDir + "err", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_DOWNLOAD_FILE);
    }
}

TEST_F(S3Test, SuccesfulDirectoryUpload) {
    DummyDirectory dir(_tempTestDir, "turingS3DirTest");

    Aws::S3Crt::Model::PutObjectResult putResult;

    Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);
    Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
    Aws::S3Crt::Model::GetObjectOutcome getOutcome;

    S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
    S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
    S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

    auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
    ASSERT_TRUE(res);
}

TEST_F(S3Test, UnsuccesfulDirectoryUpload) {
    {
        Aws::S3Crt::Model::PutObjectResult putResult;

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadDirectory("/does/not/exist", "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::DIRECTORY_NOT_FOUND);
    }

    {
        DummyDirectory dir(_tempTestDir, "turingS3DirTest");

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        DummyDirectory dir(_tempTestDir, "turingS3DirTest");

        const auto errorType = Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    {
        DummyDirectory dir(_tempTestDir, "turingS3DirTest");

        const auto errorType = Aws::S3Crt::S3CrtErrors::UNKNOWN;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        s3Err.SetResponseCode(Aws::Http::HttpResponseCode::PRECONDITION_FAILED);

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::DIRECTORY_EXISTS);
    }

    // Covers Unspecified S3 Error Case
    {
        DummyDirectory dir(_tempTestDir, "turingS3DirTest");

        const auto errorType = Aws::S3Crt::S3CrtErrors::UNKNOWN;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_UPLOAD_DIRECTORY);
    }
}

TEST_F(S3Test, SuccesfulDirectoryDownload) {
    Aws::S3Crt::Model::GetObjectResult getResult;
    Aws::S3Crt::Model::ListObjectsV2Result listResult;

    std::vector<std::string> keyNames = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};
    std::vector<std::string> keyResults;
    Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
    for (size_t i = 0; i < keyNames.size(); i++) {
        objects[i].SetKey(keyNames[i]);
    }
    listResult.SetContents(objects);

    Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
    Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
    Aws::S3Crt::Model::PutObjectOutcome putOutcome;

    S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
    S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
    S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

    auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
    ASSERT_TRUE(res);
}

TEST_F(S3Test, UnsuccesfulDirectoryDownload) {
    {
        // Test for when S3 Directory does not exist
        Aws::S3Crt::Model::GetObjectResult getResult;
        Aws::S3Crt::Model::ListObjectsV2Result listResult;

        Aws::Vector<Aws::S3Crt::Model::Object> objects;
        listResult.SetContents(objects);

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_DIRECTORY_NAME);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Result listResult;

        std::vector<std::string> keyNames = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};
        std::vector<std::string> keyResults;
        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }
        listResult.SetContents(objects);

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::NO_SUCH_KEY;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Result listResult;

        std::vector<std::string> keyNames = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};
        std::vector<std::string> keyResults;
        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }
        listResult.SetContents(objects);

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_DOWNLOAD_DIRECTORY);
    }

    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Result listResult;

        std::vector<std::string> keyNames = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};
        std::vector<std::string> keyResults;
        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }
        listResult.SetContents(objects);

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    // Covers Unspecified S3 Error Case
    {
        const auto errorType = Aws::S3Crt::S3CrtErrors::UNKNOWN;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Result listResult;

        std::vector<std::string> keyNames = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};
        std::vector<std::string> keyResults;
        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }
        listResult.SetContents(objects);

        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> turingS3Client(clientWrapper);

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_DOWNLOAD_DIRECTORY);
    }
}
