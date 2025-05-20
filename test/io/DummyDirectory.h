#pragma once

#include <string>
#include <filesystem>
#include <fstream>
#include <sys/unistd.h>

class DummyDirectory {
public:
    explicit DummyDirectory(const std::string& parentDir, const std::string& dirName, int n = 3)
        : _path(parentDir + dirName + "/"),
          _numFolders(n)
    {
        init();
    }


    ~DummyDirectory() {
        std::filesystem::remove_all(_path);
    }

    std::string& getPath() { return _path; }

    void createSubDir(const std::string& dirPath) {
        std::filesystem::create_directories(_path + dirPath);
    }

    void createFile(const std::string& filePath) {
        std::ofstream create(_path + filePath);
        create << "randText";
        create.flush();
        create.close();
    }

private:
    void init() {
        std::filesystem::create_directories(_path);
        for (int i = 1; i <= _numFolders; i++) {
            std::string fileName = "file" + std::to_string(i);
            std::string filePath = _path + fileName;
            std::string fileText = "this is file " + std::to_string(i);
            std::ofstream create(filePath);
            create << fileText;
            create.flush();
            create.close();
        }
    }

    std::string _path;
    int _numFolders;
};
