#include "Neo4JHttpRequest.h"

#include <cstdlib>

#include "BioLog.h"
#include "FileUtils.h"
#include "MsgImport.h"
#include "TimerStat.h"
#include <curl/curl.h>

using namespace Log;

static size_t writeCallback(char* contents, size_t size, size_t nmemb,
                            std::string* userp) {

    userp->append(contents, nmemb);
    return size * nmemb;
}

Neo4JHttpRequest::Neo4JHttpRequest(Neo4JHttpRequest::RequestProps&& props)
    : _url(props.host + ":" + std::to_string(props.port) +
           "/db/data/transaction/commit"),
      _username(props.user),
      _password(props.password),
      _statement(props.statement),
      _silent(props.silent)
{
}

Neo4JHttpRequest::Neo4JHttpRequest(std::string&& statement)
    : Neo4JHttpRequest({statement})
{
}

Neo4JHttpRequest::Neo4JHttpRequest(Neo4JHttpRequest&& other)
    : 
    _url(std::move(other._url)),
    _username(std::move(other._username)),
    _password(std::move(other._password)),
    _statement(std::move(other._statement)),
    _jsonRequest(std::move(other._jsonRequest)),
    _data(std::move(other._data)),
    _isReady(std::move(other._isReady)),
    _result(std::move(other._result)),
    _silent(std::move(other._silent))
{
}

Neo4JHttpRequest::~Neo4JHttpRequest() {
}

void Neo4JHttpRequest::exec() {
    TimerStat timer{"Neo4j HTTP Request: " + _statement};
    _data = "";
    CURL* curl = curl_easy_init();

    if (!curl) {
        _result = false;
        setReady();
        return;
    }

    _jsonRequest =
        "{\"statements\": [{\"statement\": \"" + _statement + "\"}]}";

    std::string userPwd = _username + ":" + _password;

    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, _url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_USERPWD, userPwd.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &_data);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, _jsonRequest.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "curl/7.81.0");
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curl, CURLOPT_FTP_SKIP_PASV_IP, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);

    if (!_silent) {
        BioLog::log(msg::INFO_NEO4J_HTTP_REQUEST() << _statement);
    }
    CURLcode res = curl_easy_perform(curl);

    if (res != 0) {
        BioLog::log(msg::ERROR_NEO4J_HTTP_REQUEST() << _statement);
        _result = false;
        setReady();
        return;
    }

    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);
    _result = true;
    setReady();
}

bool Neo4JHttpRequest::writeToFile(const std::filesystem::path& path) const {
    return FileUtils::writeFile(path, _data);
}

void Neo4JHttpRequest::clear() {
    _data.clear();
    _data.resize(0);
}

void Neo4JHttpRequest::reportError() const {
    BioLog::log(msg::ERROR_NEO4J_BAD_CURL_REQUEST() << _statement);
}

void Neo4JHttpRequest::setReady() {
    std::unique_lock lock(_readyMutex);
    _isReady = true;
    _readyCond.notify_all();
}

void Neo4JHttpRequest::waitReady() {
    std::unique_lock lock(_readyMutex);
    while (!_isReady) {
        _readyCond.wait(lock);
    }
}
