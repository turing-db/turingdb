#include "FileCache.h"
#include "TuringTest.h"
#include "DummyDirectory.h"
#include "Path.h"
#include "AwsS3ClientWrapper.h"

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

        Aws::S3Crt::Model::ListObjectsV2Result listResult;
        std::vector<std::string> commonPrefixNames = {"dir0/dir0.5/graph1/", "dir0/dir0.5/graph2/", "dir0/dir0.5/graph3/"};
        std::vector<std::string> expectedGraphs = {"graph1", "graph2", "graph3"};

        Aws::Vector<Aws::S3Crt::Model::CommonPrefix> commonPrefixes(commonPrefixNames.size());
        for (size_t i = 0; i < commonPrefixNames.size(); i++) {
            commonPrefixes[i].SetPrefix(commonPrefixNames[i]);
        }

        listResult.SetCommonPrefixes(commonPrefixes);

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        std::vector<std::string> keyNames = {"testUser1/graphs/graph1/obj1", "testUser1/graphs/graph1/obj2", "testUser1/graphs/graph1/obj3"};
        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }
        Aws::S3Crt::Model::ListObjectsV2Result listResult;
        listResult.SetContents(objects);

        Aws::S3Crt::Model::GetObjectResult getResult;

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        std::vector<std::string> keyNames = {"testUser1/graphs/graph1/obj1", "testUser1/graphs/graph1/obj2", "testUser1/graphs/graph1/obj3"};
        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }
        Aws::S3Crt::Model::ListObjectsV2Result listResult;
        listResult.SetContents(objects);

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::PutObjectResult putResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::ListObjectsV2Result listResult;
        std::vector<std::string> commonPrefixNames = {"dir0/dir0.5/folder1/", "dir0/dir0.5/folder2/", "dir0/dir0.5/folder3/"};
        std::vector<std::string> folderNames = {"folder1", "folder2", "folder3"};
        std::vector<std::string> keyNames = {"dir0/dir1/file1", "dir0/file2", "file3"};
        std::vector<std::string> fileNames = {"file1", "file2", "file3"};

        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }

        Aws::Vector<Aws::S3Crt::Model::CommonPrefix> commonPrefixes(commonPrefixNames.size());
        for (size_t i = 0; i < commonPrefixNames.size(); i++) {
            commonPrefixes[i].SetPrefix(commonPrefixNames[i]);
        }

        listResult.SetCommonPrefixes(commonPrefixes);
        listResult.SetContents(objects);

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::PutObjectResult putResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::PutObjectResult putResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::PutObjectResult putResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::GetObjectResult getResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::GetObjectResult getResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::PutObjectResult putResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::PutObjectResult putResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::PutObjectResult putResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(putResult);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome(s3Err);

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome;
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        std::vector<std::string> keyNames = {"testUser1/data/dir2/obj1", "testUser1/data/dir2/obj2", "testUser1/data/dir2/obj3"};
        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }
        Aws::S3Crt::Model::ListObjectsV2Result listResult;
        listResult.SetContents(objects);

        Aws::S3Crt::Model::GetObjectResult getResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        Aws::S3Crt::Model::GetObjectResult getResult;
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome;
        Aws::S3Crt::Model::GetObjectOutcome getOutcome(std::move(getResult));
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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

        std::vector<std::string> keyNames = {"testUser1/data/dir2/obj1", "testUser1/data/dir2/obj2", "testUser1/data/dir2/obj3"};
        Aws::Vector<Aws::S3Crt::Model::Object> objects(keyNames.size());
        for (size_t i = 0; i < keyNames.size(); i++) {
            objects[i].SetKey(keyNames[i]);
        }
        Aws::S3Crt::Model::ListObjectsV2Result listResult;
        listResult.SetContents(objects);

        const auto errorType = Aws::S3Crt::S3CrtErrors::ACCESS_DENIED;
        Aws::S3Crt::S3CrtError s3Err(Aws::Client::AWSError(errorType, false));
        Aws::S3Crt::Model::ListObjectsV2Outcome listOutcome(listResult);
        Aws::S3Crt::Model::GetObjectOutcome getOutcome(s3Err);
        Aws::S3Crt::Model::PutObjectOutcome putOutcome;

        S3::MockS3Client mockClient(putOutcome, getOutcome, listOutcome);
        S3::AwsS3ClientWrapper<S3::MockS3Client> clientWrapper(mockClient);
        S3::TuringS3Client<S3::AwsS3ClientWrapper<S3::MockS3Client>> TuringClient(std::move(clientWrapper));

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
