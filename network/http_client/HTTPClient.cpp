#include "HTTPClient.h"

#include <curl/curl.h>

using namespace net::HTTP;

namespace {

size_t writeCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    HTTPClient::ResponseBuffer* readBuffer = (HTTPClient::ResponseBuffer*)userp;
    const uint8_t* byteContents = (uint8_t*)contents;

    auto& data = readBuffer->_data;
    data.insert(data.end(), byteContents, byteContents+size*nmemb);

    return size*nmemb;
}

}

HTTPClient::HTTPClient()
{
    _curl = curl_easy_init();
}

HTTPClient::~HTTPClient() {
    curl_easy_cleanup(_curl);
    _curl = nullptr;
}

Status HTTPClient::fetch(std::string_view url,
                         std::string_view reqData,
                         ResponseBuffer& resp) {
    resp.clear();

    curl_easy_reset(_curl);
    curl_easy_setopt(_curl, CURLOPT_URL, url.data());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, &resp);
    
    const CURLcode res = curl_easy_perform(_curl);
    if (res != CURLE_OK) {
        long httpCode = 0;
        curl_easy_getinfo(_curl, CURLINFO_RESPONSE_CODE, &httpCode);
        return net::HTTP::codeToStatus((size_t)httpCode);
    }

    return Status::OK;
}