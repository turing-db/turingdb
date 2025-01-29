#pragma once

#include <string>
#include <exception>

namespace db {

class PipelineException : public std::exception {
public:
    explicit PipelineException(const std::string& msg);
    ~PipelineException() noexcept override;

    const char* what() const noexcept override;

private:
    std::string _msg;
};

} 
