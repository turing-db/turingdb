#pragma once

#include <string>
#include <vector>
#include <memory>

#include "Path.h"
#include "metadata/PropertyType.h"

namespace db {

class ASTContext;
class DeclContext;
class ReturnProjection;
class MatchTargets;
class CreateTarget;
class CreateTargets;

class QueryCommand {
public:
    friend ASTContext;

    enum class Kind {
        MATCH_COMMAND = 0,
        CREATE_COMMAND,
        COMMIT_COMMAND,
        CREATE_GRAPH_COMMAND,
        LIST_GRAPH_COMMAND,
        LOAD_GRAPH_COMMAND,
        IMPORT_GRAPH_COMMAND,
        EXPLAIN_COMMAND,
        HISTORY_COMMAND,
        CHANGE_COMMAND,
        CALL_COMMAND,
        S3CONNECT_COMMAND,
        S3TRANSFER_COMMAND,
    };

    virtual Kind getKind() const = 0;

protected:
    QueryCommand() = default;
    virtual ~QueryCommand() = default;
    void registerCmd(ASTContext* ctxt);
};

class MatchCommand : public QueryCommand {
public:
    static MatchCommand* create(ASTContext* ctxt);

    DeclContext* getDeclContext() const { return _declContext.get(); }

    Kind getKind() const override { return Kind::MATCH_COMMAND; }

    ReturnProjection* getProjection() const { return _proj; }
    MatchTargets* getMatchTargets() const { return _matchTargets; }

    void setProjection(ReturnProjection* proj) { _proj = proj; }
    void setMatchTargets(MatchTargets* targets) { _matchTargets = targets; }

private:
    std::unique_ptr<DeclContext> _declContext;
    ReturnProjection* _proj {nullptr};
    MatchTargets* _matchTargets {nullptr};

    MatchCommand();
    ~MatchCommand() override;
};

class CreateCommand : public QueryCommand {
public:
    static CreateCommand* create(ASTContext* ctxt, CreateTargets* targets);

    DeclContext* getDeclContext() const { return _declContext.get(); }
    const CreateTargets& createTargets() const { return *_createTargets; }

    Kind getKind() const override { return Kind::CREATE_COMMAND; }

private:
    std::unique_ptr<DeclContext> _declContext;
    CreateTargets* _createTargets {nullptr};

    explicit CreateCommand(CreateTargets* targets);
    ~CreateCommand() override;
};

class CommitCommand : public QueryCommand {
public:
    static CommitCommand* create(ASTContext* ctxt);

    Kind getKind() const override { return Kind::COMMIT_COMMAND; }

private:
    CommitCommand();
    ~CommitCommand() override;
};

class CreateGraphCommand : public QueryCommand {
public:

    static CreateGraphCommand* create(ASTContext* ctxt, const std::string& name);

    Kind getKind() const override { return Kind::CREATE_GRAPH_COMMAND; }

    const std::string& getName() const { return _name; }

private:
    std::string _name;

    CreateGraphCommand(const std::string& name);
    ~CreateGraphCommand() override;
};

class ListGraphCommand : public QueryCommand {
public:
    static ListGraphCommand* create(ASTContext* ctxt);

    Kind getKind() const override { return Kind::LIST_GRAPH_COMMAND; }

private:
    ListGraphCommand() = default;
    ~ListGraphCommand() override = default;
};

class LoadGraphCommand : public QueryCommand {
public:
    static LoadGraphCommand* create(ASTContext* ctxt, const std::string& graphName);
    static LoadGraphCommand* create(ASTContext* ctxt, const fs::Path& filePath, const std::string& graphName);

    Kind getKind() const override { return Kind::LOAD_GRAPH_COMMAND; }

    const fs::Path& getFilePath() const { return _filePath; }
    const std::string& getGraphName() const { return _graphName; }

private:
    fs::Path _filePath;
    std::string _graphName;

    LoadGraphCommand(const std::string& graphName);
    LoadGraphCommand(const fs::Path& filePath, const std::string& graphName);
    ~LoadGraphCommand() override = default;
};

class ImportGraphCommand : public QueryCommand {
public:
    static ImportGraphCommand* create(ASTContext* ctxt, const fs::Path& filePath, const std::string& graphName);

    Kind getKind() const override { return Kind::IMPORT_GRAPH_COMMAND; }

    const fs::Path& getFilePath() const { return _filePath; }
    const std::string& getGraphName() const { return _graphName; }

private:
    fs::Path _filePath;
    std::string _graphName;

    ImportGraphCommand(const fs::Path& filePath, const std::string& graphName);
    ~ImportGraphCommand() override = default;
};

class ExplainCommand : public QueryCommand {
public:
    static ExplainCommand* create(ASTContext* ctxt, QueryCommand* query);

    Kind getKind() const override { return Kind::EXPLAIN_COMMAND; }

    QueryCommand* getQuery() const { return _query; }

private:
    QueryCommand* _query {nullptr};

    ExplainCommand(QueryCommand* query);
    ~ExplainCommand() override = default;
};

class HistoryCommand : public QueryCommand {
public:
    static HistoryCommand* create(ASTContext* ctxt);

    Kind getKind() const override { return Kind::HISTORY_COMMAND; }

private:
    HistoryCommand();
    ~HistoryCommand() override = default;
};

class CallCommand : public QueryCommand {
public:
    enum class Type {
        LABELS = 0,
        LABELSETS,
        PROPERTIES,
        EDGETYPES,

        SIZE
    };

    constexpr std::span<const std::string_view> getColNames() const {
        return colNameTable[static_cast<size_t>(Type::SIZE)];
    }

    constexpr std::span<const ValueType> getColTypes() const {
        return colTypeTable[static_cast<size_t>(Type::SIZE)];
    }

    static CallCommand* create(ASTContext* ctx, Type);

    Kind getKind() const override { return Kind::CALL_COMMAND; }
    Type getType() const { return _type; }

private:
    Type _type;

    CallCommand() = delete;
    CallCommand(Type type);
    //~CallCommand() override = default;

    static constexpr std::array<std::string_view, 2> labelsColNames = {"LabelID", "LabelName"};
    static constexpr std::array<std::string_view, 2> labelSetsColNames = {"LabelSetID", "LabelName"};
    static constexpr std::array<std::string_view, 2> edgeTypesColNames = {"EdgeTypeID", "EdgeTypeName"};
    static constexpr std::array<std::string_view, 3> propertiesColNames = {"PropertyID", "PropertyName", "PropertyType"};

    static constexpr std::array<std::span<const std::string_view>, static_cast<size_t>(Type::SIZE)> colNameTable = {
        {std::span {labelsColNames},
         std::span {labelSetsColNames},
         std::span {propertiesColNames},
         std::span {edgeTypesColNames}}
    };

    static constexpr std::array<ValueType, 2> labelsColTypes = {ValueType::UInt64, ValueType::String};
    static constexpr std::array<ValueType, 2> labelSetsColTypes = {ValueType::UInt64, ValueType::String};
    static constexpr std::array<ValueType, 2> edgeTypesColTypes = {ValueType::UInt64, ValueType::String};
    static constexpr std::array<ValueType, 3> propertiesColTypes = {ValueType::UInt64, ValueType::String, ValueType::String};

    static constexpr std::array<std::span<const ValueType>, static_cast<size_t>(Type::SIZE)> colTypeTable = {
        {std::span {labelsColTypes},
         std::span {labelSetsColTypes},
         std::span {propertiesColTypes},
         std::span {edgeTypesColTypes}}
    };
};

class S3ConnectCommand : public QueryCommand {
public:
    const std::string& getAccessId() const {
        return _accessId;
    }

    const std::string& getSecretKey() const {
        return _secretKey;
    }

    const std::string& getRegion() const {
        return _region;
    }

    static S3ConnectCommand* create(ASTContext* ctx, std::string& accessId, std::string& secretKey, std::string& region);
    static S3ConnectCommand* create(ASTContext* ctx);

    Kind getKind() const override {
        return QueryCommand::Kind::S3CONNECT_COMMAND;
    }

private:
    S3ConnectCommand(std::string& accessId, std::string& secretKey, std::string& region);
    S3ConnectCommand() = default;

    std::string _accessId;
    std::string _secretKey;
    std::string _region;
};

class S3TransferCommand : public QueryCommand {
public:
    enum class Direction : uint8_t {
        PULL = 0,
        PUSH
    };

    std::string_view getS3URL() const {
        return _s3URL;
    }

    const std::string& getLocalDir() const {
        return _localDir;
    }

    Direction getTransferDir() const {
        return _transferDirection;
    }

    std::string_view& getBucket() {
        return _s3Bucket;
    }

    std::string_view& getPrefix() {
        return _s3Prefix;
    }

    std::string_view& getFile() {
        return _s3File;
    }

    const std::string_view& getBucket() const {
        return _s3Bucket;
    }

    const std::string_view& getPrefix() const {
        return _s3Prefix;
    }

    const std::string_view& getFile() const {
        return _s3File;
    }

    static S3TransferCommand* create(ASTContext* ctx, Direction _transferDir, const std::string& s3URL, const std::string& localDir);

    Kind getKind() const override {
        return QueryCommand::Kind::S3TRANSFER_COMMAND;
    }

private:
    S3TransferCommand(Direction _transferDir, const std::string& s3URL, const std::string& localDir);
    S3TransferCommand() = delete;

    Direction _transferDirection;

    std::string _s3URL;
    std::string _localDir;

    std::string_view _s3Bucket;
    std::string_view _s3Prefix;
    std::string_view _s3File;
};
}
