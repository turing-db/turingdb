#include "Message.h"

using namespace Log;

Message::Message(MessageID msgID, Severity severity, const std::string& format)
    : _id(msgID), _severity(severity), _format(format)
{
}

Message::~Message() {
}

Message& Message::operator<<(std::string_view str) {
    return (*this << std::string(str));
}

Message& Message::operator<<(const std::string& value) {
    _argStorage._args[_argStorage._size] = value;
    _argStorage._size++;
    return *this;
}

Message& Message::operator<<(std::string&& value) {
    _argStorage._args[_argStorage._size] = std::move(value);
    _argStorage._size++;
    return *this;
}

Message& Message::operator<<(long int value) {
    _argStorage._args[_argStorage._size] = std::to_string(value);
    _argStorage._size++;
    return *this;
}
