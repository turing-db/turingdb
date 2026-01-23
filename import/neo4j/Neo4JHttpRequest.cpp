#include "Neo4JHttpRequest.h"

#include <curl/curl.h>
#include <spdlog/spdlog.h>

#include "TimerStat.h"

namespace {

size_t writeCallback(char* contents, size_t size, size_t nmemb,
                     std::string* userp) {

    userp->append(contents, nmemb);
    return size * nmemb;
}

}

namespace db {

bool Neo4JHttpRequest::exec() {
    TimerStat timer {"Neo4j HTTP Request: " + _statement};
    CURL* curl = curl_easy_init();

    if (!curl) {
        return false;
    }

    const std::string jsonData = R"({"statements": [{"statement": ")" + _statement + "\"}]}";
    const std::string userPwd = _username + ":" + _password;
    const std::string url = _url + _urlSuffix;

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_PORT, _port);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_USERPWD, userPwd.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &_response);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "curl/7.81.0");
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, _method.c_str());
    curl_easy_setopt(curl, CURLOPT_FTP_SKIP_PASV_IP, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);

    if (_method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonData.c_str());
    }

    CURLcode res = curl_easy_perform(curl);

    if (res != 0) {
        spdlog::error("Error in request to Neo4j '{}'", curl_easy_strerror(res));
        return false;
    }

    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);
    return true;
}

bool Neo4JHttpRequest::execStatic(std::string* response,
                             const std::string& url,
                             const std::string& urlSuffix,
                             const std::string& username,
                             const std::string& password,
                             uint64_t port,
                             std::string_view method,
                             const std::string& statement) {
    TimerStat timer {"Neo4j HTTP Request: " + statement};
    CURL* curl = curl_easy_init();

    if (!curl) {
        return false;
    }

    const std::string jsonData = R"({"statements": [{"statement": ")" + statement + "\"}]}";
    const std::string userPwd = username + ":" + password;
    const std::string finalUrl = url + urlSuffix;

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_PORT, port);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_USERPWD, userPwd.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, response);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "curl/7.81.0");
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.data());
    curl_easy_setopt(curl, CURLOPT_FTP_SKIP_PASV_IP, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);

    if (method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonData.c_str());
    }

    const CURLcode res = curl_easy_perform(curl);

    if (res != 0) {
        spdlog::error("Error in request to Neo4j '{}'", curl_easy_strerror(res));
        return false;
    }

    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);
    return true;
}

}
