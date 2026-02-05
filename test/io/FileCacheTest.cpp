#include "FileCache.h"
#include "TuringTest.h"
#include "DummyDirectory.h"
#include "Path.h"
#include "MinioS3ClientWrapper.h"
#include "MockS3Client.h"

using namespace turing::test;

class FileCacheTest : public TuringTest {
protected:
    std::string _tempTestDir = std::filesystem::current_path().string() + "/" + _testName + ".tmp/";
    void initialize() override {
        std::filesystem::create_directory(_tempTestDir);
    }

    void terminate() override {
        std::filesystem::remove_all(_tempTestDir);
    }
};

TEST_F(FileCacheTest, SuccesfulListGraphs) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        listResult.success = true;
        listResult.commonPrefixes = {"dir0/dir0.5/graph1/", "dir0/dir0.5/graph2/", "dir0/dir0.5/graph3/"};
        std::vector<std::string> expectedGraphs = {"graph1", "graph2", "graph3"};

        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<std::string> graphs;
        auto res = cache.listGraphs(graphs);
        ASSERT_TRUE(res);
        EXPECT_EQ(expectedGraphs, graphs);
    }
}

TEST_F(FileCacheTest, UnsuccesfulListGraphs) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        listResult.success = false;
        listResult.errorType = S3::S3ClientErrorType::CANNOT_LIST_FOLDERS;

        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<std::string> graphs;
        auto res = cache.listGraphs(graphs);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::LIST_GRAPHS_FAILED);
    }
}

TEST_F(FileCacheTest, SuccesfulListLocalGraphs) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("graphs/graph1");
        dir.createSubDir("graphs/graph2");
        dir.createSubDir("graphs/graph3");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");
        std::vector<fs::Path> result = {(graphPath / "graph1"), (graphPath / "graph2"), (graphPath / "graph3")};

        S3::MockListResult listResult;
        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> graphs;
        auto res = cache.listLocalGraphs(graphs);
        ASSERT_TRUE(res);
        EXPECT_EQ(std::set(result.begin(), result.end()), std::set(graphs.begin(), graphs.end()));
    }
}

TEST_F(FileCacheTest, SuccesfulLoadGraphs) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("graphs/graph1");
        dir.createSubDir("graphs/graph2");
        dir.createSubDir("graphs/graph3");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> graphs;
        auto res = cache.loadGraph("graph1");
        ASSERT_TRUE(res);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {"testUser1/graphs/graph1/obj1", "testUser1/graphs/graph1/obj2", "testUser1/graphs/graph1/obj3"};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = true;
        downloadResult.content = "";

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> graphs;
        auto res = cache.loadGraph("graph1");
        ASSERT_TRUE(res);
    }
}

TEST_F(FileCacheTest, UnsuccesfulLoadGraphs) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {"testUser1/graphs/graph1/obj1", "testUser1/graphs/graph1/obj2", "testUser1/graphs/graph1/obj3"};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> graphs;
        auto res = cache.loadGraph("graph1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::GRAPH_LOAD_FAILED);
    }
}

TEST_F(FileCacheTest, SuccesfulSaveGraphs) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("graphs/graph1");
        dir.createSubDir("graphs/graph2");
        dir.createSubDir("graphs/graph3");
        dir.createFile("graphs/graph1/file1");
        dir.createFile("graphs/graph1/file2");
        dir.createFile("graphs/graph1/file3");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> graphs;
        auto res = cache.saveGraph("graph1");
        ASSERT_TRUE(res);
    }
}

TEST_F(FileCacheTest, UnsuccesfulSaveGraphs) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("graphs/graph2");
        dir.createSubDir("graphs/graph3");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> graphs;
        auto res = cache.saveGraph("graph1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::FAILED_TO_FIND_GRAPH);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("graphs/graph1");
        dir.createSubDir("graphs/graph2");
        dir.createSubDir("graphs/graph3");
        dir.createFile("graphs/graph1/file1");
        dir.createFile("graphs/graph1/file2");
        dir.createFile("graphs/graph1/file3");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> graphs;
        auto res = cache.saveGraph("graph1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::GRAPH_SAVE_FAILED);
    }
}

TEST_F(FileCacheTest, SuccesfulListData) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        listResult.success = true;
        listResult.commonPrefixes = {"dir0/dir0.5/folder1/", "dir0/dir0.5/folder2/", "dir0/dir0.5/folder3/"};
        listResult.keys = {"dir0/dir1/file1", "dir0/file2", "file3"};

        std::vector<std::string> folderNames = {"folder1", "folder2", "folder3"};
        std::vector<std::string> fileNames = {"file1", "file2", "file3"};

        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<std::string> folderResults;
        std::vector<std::string> fileResults;
        auto res = cache.listData(fileResults, folderResults);
        ASSERT_TRUE(res);
        EXPECT_EQ(fileNames, fileResults);
        EXPECT_EQ(folderNames, folderResults);
    }
}

TEST_F(FileCacheTest, UnsuccesfulListData) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        listResult.success = false;
        listResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<std::string> folderResults;
        std::vector<std::string> fileResults;
        auto res = cache.listData(fileResults, folderResults);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::LIST_DATA_FAILED);
    }
}

TEST_F(FileCacheTest, SuccesfulListLocalData) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1");
        dir.createSubDir("data/dir2");
        dir.createSubDir("data/dir3");
        dir.createFile("data/file1");
        dir.createFile("data/file2");
        dir.createFile("data/file3");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");
        std::vector<fs::Path> folders = {(dataPath / "dir1"), (dataPath / "dir2"), (dataPath / "dir3")};
        std::vector<fs::Path> files = {(dataPath / "file1"), (dataPath / "file2"), (dataPath / "file3")};

        S3::MockListResult listResult;
        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> folderResults;
        std::vector<fs::Path> fileResults;
        auto res = cache.listLocalData(fileResults, folderResults);
        ASSERT_TRUE(res);
        EXPECT_EQ(std::set(files.begin(), files.end()), std::set(fileResults.begin(), fileResults.end()));
        EXPECT_EQ(std::set(folders.begin(), folders.end()), std::set(folderResults.begin(), folderResults.end()));
    }
}

TEST_F(FileCacheTest, UnsuccesfulListLocalData) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1");
        dir.createSubDir("data/dir2");
        dir.createSubDir("data/dir3");
        dir.createFile("data/file1");
        dir.createFile("data/file2");
        dir.createFile("data/file3");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        S3::MockUploadResult uploadResult;
        S3::MockDownloadResult downloadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        std::vector<fs::Path> folderResults;
        std::vector<fs::Path> fileResults;
        std::string subdir = "nonexistent";
        auto res = cache.listLocalData(fileResults, folderResults, subdir);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::DIRECTORY_DOES_NOT_EXIST);
    }
}

TEST_F(FileCacheTest, SuccesfulSaveDataFile) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createFile("data/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.saveDataFile("file1");
        ASSERT_TRUE(res);
    }
}

TEST_F(FileCacheTest, UnsuccesfulSaveDataFile) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createFile("data/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.saveDataFile("file10");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::FAILED_TO_FIND_DATA_FILE);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1");
        dir.createFile("data/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.saveDataFile("dir1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::FILE_PATH_IS_DIRECTORY);
    }
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createFile("data/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.saveDataFile("file1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::DATA_FILE_SAVE_FAILED);
    }
}

TEST_F(FileCacheTest, SuccesfulLoadDataFile) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createFile("data/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        S3::MockDownloadResult downloadResult;
        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.loadDataFile("file1");
        ASSERT_TRUE(res);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createFile("data/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockDownloadResult downloadResult;
        downloadResult.success = true;
        downloadResult.content = "";

        S3::MockListResult listResult;
        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.loadDataFile("file2");
        ASSERT_TRUE(res);
    }
}

TEST_F(FileCacheTest, UnsuccesfulLoadDataFile) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1");
        dir.createFile("data/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockDownloadResult downloadResult;
        downloadResult.success = true;
        downloadResult.content = "";

        S3::MockListResult listResult;
        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.loadDataFile("dir1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::FILE_PATH_IS_DIRECTORY);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockListResult listResult;
        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.loadDataFile("file1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::DATA_FILE_LOAD_FAILED);
    }
}

TEST_F(FileCacheTest, SuccesfulSaveDataDirectory) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1/");
        dir.createFile("data/dir1/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.saveDataDirectory("dir1");
        ASSERT_TRUE(res);
    }
}

TEST_F(FileCacheTest, UnsuccesfulSaveDataDirectory) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1/");
        dir.createFile("data/dir1/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.saveDataDirectory("dir2");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::FAILED_TO_FIND_DATA_DIRECTORY);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1/");
        dir.createFile("data/dir1/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = true;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.saveDataDirectory("dir1/file1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::DIRECTORY_PATH_IS_FILE);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1/");
        dir.createFile("data/dir1/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockUploadResult uploadResult;
        uploadResult.success = false;
        uploadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockDownloadResult downloadResult;
        S3::MockListResult listResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.saveDataDirectory("dir1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::DATA_DIRECTORY_SAVE_FAILED);
    }
}

TEST_F(FileCacheTest, SuccesfulLoadDataDirectory) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1/");
        dir.createFile("data/dir1/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        S3::MockDownloadResult downloadResult;
        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.loadDataDirectory("dir1");
        ASSERT_TRUE(res);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1/");
        dir.createFile("data/dir1/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {"testUser1/data/dir2/obj1", "testUser1/data/dir2/obj2", "testUser1/data/dir2/obj3"};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = true;
        downloadResult.content = "";

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.loadDataDirectory("dir2");
        ASSERT_TRUE(res);
    }
}

TEST_F(FileCacheTest, UnsuccesfulLoadDataDirectory) {
    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1/");
        dir.createFile("data/dir1/file1");
        dir.createFile("data/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockDownloadResult downloadResult;
        downloadResult.success = true;
        downloadResult.content = "";

        S3::MockListResult listResult;
        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.loadDataDirectory("file1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::DIRECTORY_PATH_IS_FILE);
    }

    {
        DummyDirectory dir(_tempTestDir, "FileCacheTest");
        dir.createSubDir("data");
        dir.createSubDir("graphs");
        dir.createSubDir("data/dir1/");
        dir.createFile("data/dir1/file1");
        fs::Path graphPath = fs::Path(dir.getPath() + "graphs");
        fs::Path dataPath = fs::Path(dir.getPath() + "data");

        S3::MockListResult listResult;
        listResult.success = true;
        listResult.keys = {"testUser1/data/dir2/obj1", "testUser1/data/dir2/obj2", "testUser1/data/dir2/obj3"};

        S3::MockDownloadResult downloadResult;
        downloadResult.success = false;
        downloadResult.errorType = S3::S3ClientErrorType::ACCESS_DENIED;

        S3::MockUploadResult uploadResult;

        S3::MockS3Client mockClient(uploadResult, downloadResult, listResult);
        S3::TuringS3Client<S3::MockS3Client> TuringClient(std::move(mockClient));

        db::FileCache cache = db::FileCache(graphPath, dataPath, TuringClient);

        auto res = cache.loadDataDirectory("dir1/file1");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), db::FileCacheErrorType::DIRECTORY_PATH_IS_FILE);
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 4;
    });
}
