#ifndef _BIO_LOG_MESSAGE_
#define _BIO_LOG_MESSAGE_

#include <string>
#include <string_view>

#define DEFINE_MESSAGE(Name, Type, Code, MsgStr)     \
    class Name : public Log::Message {               \
    public:                                          \
        Name()                                       \
            : Message(Code, Message::Type, MsgStr) { \
        }                                            \
    };

#define MSG_FATAL(Name, Code, MsgStr) DEFINE_MESSAGE(Name, FATAL, Code, MsgStr)
#define MSG_ERROR(Name, Code, MsgStr) DEFINE_MESSAGE(Name, ERROR, Code, MsgStr)
#define MSG_WARNING(Name, Code, MsgStr) DEFINE_MESSAGE(Name, WARNING, Code, MsgStr)
#define MSG_INFO(Name, Code, MsgStr) DEFINE_MESSAGE(Name, INFO, Code, MsgStr)

// MsgCommon    1000
// MsgImport    2000
// MsgUIServer  3000
// MsgRun       4000
// MsgDB        5000
// MsgRPCServer 6000
// MsgShell     7000
// MsgWRT       8000
// MsgLLM       9000

namespace Log {

struct MessageArgStorage {
    static constexpr size_t MAX_ARGS = 8;

    std::string _args[MAX_ARGS];
    unsigned _size {0};
};

class Message {
public:
    using MessageID = unsigned;

    enum Severity {
        INFO,
        WARNING,
        ERROR,
        FATAL
    };

    ~Message();

    MessageID getID() const { return _id; }

    Severity getSeverity() const { return _severity; }

    const std::string& getFormat() const { return _format; }

    const MessageArgStorage* getArgStorage() const { return &_argStorage; }

    Message& operator<<(const std::string& value);
    Message& operator<<(std::string&& value);

    template <std::integral T>
    Message& operator<<(T value) {
        _argStorage._args[_argStorage._size] = std::to_string(value);
        _argStorage._size++;
        return *this;
    }

    template <std::floating_point T>
    Message& operator<<(T value) {
        _argStorage._args[_argStorage._size] = std::to_string(value);
        _argStorage._size++;
        return *this;
    }

protected:
    Message(MessageID msgID, Severity severity, const std::string& format);

private:
    static constexpr size_t MAX_ARGS = 8;

    const MessageID _id;
    const Severity _severity;
    const std::string _format;
    MessageArgStorage _argStorage;
};

}

#endif
