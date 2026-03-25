#pragma once

#include "TuringException.h"

/**
 * @brief Exception class to denote a wire-protocol failure: malformed framing,
 * bad handshake payload, unsupported message type, truncated query payload, etc.
 * @detail Carries no extra state beyond the message string. Catching this
 * distinctly from other TuringExceptions lets the server emit a PROTOCOL_ERROR
 * packet to the client instead of silently closing the connection.
 */
class ProtocolException : public TuringException {
public:
    explicit ProtocolException(std::string&& msg);
    ProtocolException(const ProtocolException&) = default;
    ProtocolException(ProtocolException&&) = default;
    ProtocolException& operator=(const ProtocolException&) = default;
    ProtocolException& operator=(ProtocolException&&) = default;
    ~ProtocolException() noexcept override;

    const char* what() const noexcept override;
};
