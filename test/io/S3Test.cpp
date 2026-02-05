#include "TuringS3Client.h"
#include "TuringTest.h"

#include <fstream>
#include <filesystem>
#include <sys/unistd.h>

#include "MockS3Client.h"
#include "MinioS3ClientWrapper.h"
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
    S3::MockListResult listResult;
    listResult.success = true;
    listResult.keys = {"dir0/dir1/file1", "dir0/file2", "file3"};
    listResult.commonPrefixes = {"dir0/dir0.5/dir1/", "dir0/dir0.5/dir2/", "dir0/dir0.5/dir3/"};

    std::vector<std::string> keyNames = {"dir0/dir1/file1", "dir0/file2", "file3"};
    std::vector<std::string> fileNames = {"file1", "file2", "file3"};
    std::vector<std::string> folderNames = {"dir1", "dir2", "dir3"};

    S3::MockUploadResult uploadResult;
    S3::MockDownloadResult downloadResult;

    S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
    S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

    std::vector<std::string> keyResults;
    auto res = turingS3Client.listKeys("bucketName", "prefix/", keyResults);
    ASSERT_TRUE(res);
    EXPECT_EQ(keyNames, keyResults);

    std::vector<std::string> fileResults;
    res = turingS3Client.listFiles("bucketName", "prefix/", fileResults);
    ASSERT_TRUE(res);
    EXPECT_EQ(fileNames, fileResults);

    std::vector<std::string> folderResults;
    res = turingS3Client.listFolders("bucketName", "prefix/", folderResults);
    ASSERT_TRUE(res);
    EXPECT_EQ(folderNames, folderResults);
}

TEST_F(S3Test, UnsucessfulListOperations) {
    {
        S3::MockListResult listResult;
        listResult.success = false;
        listResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

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
        S3::MockListResult listResult;
        listResult.success = false;
        listResult.errorType = S3::S3ClientErrorType::INVALID_BUCKET_NAME;

        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

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
        S3::MockListResult listResult;
        listResult.success = false;
        listResult.errorType = S3::S3ClientErrorType::CANNOT_LIST_KEYS;

        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        std::vector<std::string> keyResults;
        std::vector<std::string> fileResults;
        std::vector<std::string> folderResults;

        auto res = turingS3Client.listKeys("bucketName", "prefix/", keyResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_LIST_KEYS);

        listResult.errorType = S3::S3ClientErrorType::CANNOT_LIST_FILES;
        S3::MockS3Client mockClient2(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client2(std::move(mockClient2));

        res = turingS3Client2.listFiles("bucketName", "prefix/", fileResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_LIST_FILES);

        listResult.errorType = S3::S3ClientErrorType::CANNOT_LIST_FOLDERS;
        S3::MockS3Client mockClient3(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client3(std::move(mockClient3));

        res = turingS3Client3.listFolders("bucketName", "prefix/", folderResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_LIST_FOLDERS);
    }
}

TEST_F(S3Test, SuccesfulFileUpload) {
    S3::MockUploadResult uploadResult;
    uploadResult.success = true;

    S3::MockDownloadResult downloadResult;
    S3::MockListResult listResult;

    S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
    S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

    auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
    ASSERT_TRUE(res);
}

TEST_F(S3Test, UnsuccesfulFileUpload) {
    {
        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadFile("/does/not/exist", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::FILE_NOT_FOUND);
    }

    {
        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::INVALID_BUCKET_NAME;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    {
        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.statusCode = 412;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::FILE_EXISTS);
    }

    {
        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::CANNOT_UPLOAD_FILE;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadFile("/dev/null", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_UPLOAD_FILE);
    }
}

TEST_F(S3Test, SuccesfulFileDownload) {
    S3::MockDownloadResult downloadResult;
    downloadResult.success = true;
    downloadResult.content = "";

    S3::MockUploadResult uploadResult;
    S3::MockListResult listResult;

    S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
    S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

    auto res = turingS3Client.downloadFile("/dev/null", "bucketName", "keyName");
    ASSERT_TRUE(res);
}

TEST_F(S3Test, UnsuccesfulFileDownload) {
    {
        S3::MockDownloadResult downloadResult;
        downloadResult.success = true;
        downloadResult.content = "";

        S3::MockUploadResult uploadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadFile("/does/not/exist", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_OPEN_FILE);
    }

    {
        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockUploadResult uploadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadFile(_tempTestDir + "err", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::INVALID_KEY_NAME;

        S3::MockUploadResult uploadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadFile(_tempTestDir + "err", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_KEY_NAME);
    }

    {
        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::INVALID_BUCKET_NAME;

        S3::MockUploadResult uploadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadFile(_tempTestDir + "err", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    {
        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::CANNOT_DOWNLOAD_FILE;

        S3::MockUploadResult uploadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadFile(_tempTestDir + "err", "bucketName", "keyName");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_DOWNLOAD_FILE);
    }
}

TEST_F(S3Test, SuccesfulDirectoryUpload) {
    DummyDirectory dir(_tempTestDir, "turingS3DirTest");

    S3::MockUploadResult uploadResult;
    uploadResult.success = true;

    S3::MockDownloadResult downloadResult;
    S3::MockListResult listResult;

    S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
    S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

    auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
    ASSERT_TRUE(res);
}

TEST_F(S3Test, UnsuccesfulDirectoryUpload) {
    {
        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadDirectory("/does/not/exist", "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::DIRECTORY_NOT_FOUND);
    }

    {
        DummyDirectory dir(_tempTestDir, "turingS3DirTest");

        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        DummyDirectory dir(_tempTestDir, "turingS3DirTest");

        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::INVALID_BUCKET_NAME;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    {
        DummyDirectory dir(_tempTestDir, "turingS3DirTest");

        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.statusCode = 412;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::DIRECTORY_EXISTS);
    }

    {
        DummyDirectory dir(_tempTestDir, "turingS3DirTest");

        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::CANNOT_UPLOAD_FILE;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.uploadDirectory(dir.getPath(), "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_UPLOAD_DIRECTORY);
    }
}

TEST_F(S3Test, SuccesfulDirectoryDownload) {
    S3::MockListResult listResult;
    listResult.success = true;
    listResult.keys = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};

    S3::MockDownloadResult downloadResult;
    downloadResult.success = true;
    downloadResult.content = "";

    S3::MockUploadResult uploadResult;

    S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
    S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

    auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
    ASSERT_TRUE(res);
}

TEST_F(S3Test, UnsuccesfulDirectoryDownload) {
    {
        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = true;

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_DIRECTORY_NAME);
    }

    {
        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::ACCESS_DENIED);
    }

    {
        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::INVALID_KEY_NAME;

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_DOWNLOAD_DIRECTORY);
    }

    {
        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::INVALID_BUCKET_NAME;

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::INVALID_BUCKET_NAME);
    }

    {
        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {"prefix/dir0/dir1/file1", "prefix/dir0/file2", "prefix/file3"};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::CANNOT_DOWNLOAD_FILE;

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> turingS3Client(std::move(mockClient));

        auto res = turingS3Client.downloadDirectory(_tempTestDir, "bucketName", "prefix/");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), S3::S3ClientErrorType::CANNOT_DOWNLOAD_DIRECTORY);
    }
}
