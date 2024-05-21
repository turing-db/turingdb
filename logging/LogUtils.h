#pragma once

#include <string>

namespace logt {

void TuringHomeUndefined();

void FileNotFound(const std::string& path);
void NotADirectory(const std::string& path);
void DirectoryDoesNotExist(const std::string& path);

void CanNotRead(const std::string& path);
void CanNotWrite(const std::string& path);
void CanNotRemove(const std::string& path);
void CanNotCreateDir(const std::string& path);

void ExecutableNotFound(const std::string& cmd);
void ImpossibleToRunCommand(const std::string& cmd);

void ElapsedTime(float time, std::string_view unit);

}
