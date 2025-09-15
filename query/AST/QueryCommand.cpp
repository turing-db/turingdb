#include "QueryCommand.h"

#include "ASTContext.h"
#include "DeclContext.h"
#include "ChangeCommand.h"

using namespace db;

void QueryCommand::registerCmd(ASTContext* ctxt) {
    ctxt->addCmd(this);
}

// ReturnCommand

MatchCommand::MatchCommand()
    : _declContext(std::make_unique<DeclContext>()) 
{
}

MatchCommand::~MatchCommand() {
}

MatchCommand* MatchCommand::create(ASTContext* ctxt) {
    MatchCommand* matchCmd = new MatchCommand();
    matchCmd->registerCmd(ctxt);
    return matchCmd;
}

// CreateCommand

CreateCommand::CreateCommand(CreateTargets* targets)
    : _declContext(std::make_unique<DeclContext>()),
     _createTargets(targets)
{
}

CreateCommand::~CreateCommand() {
}

CreateCommand* CreateCommand::create(ASTContext* ctxt, CreateTargets* targets) {
    CreateCommand* cmd = new CreateCommand(targets);
    cmd->registerCmd(ctxt);
    return cmd;
}

// CommitCommand 

CommitCommand::CommitCommand()
{
}

CommitCommand::~CommitCommand() {
}

CommitCommand* CommitCommand::create(ASTContext* ctxt) {
    CommitCommand* cmd = new CommitCommand();
    cmd->registerCmd(ctxt);
    return cmd;
}

// CreateGraphCommand

CreateGraphCommand::CreateGraphCommand(const std::string& name)
    : _name(name)
{
}

CreateGraphCommand::~CreateGraphCommand() {
}

CreateGraphCommand* CreateGraphCommand::create(ASTContext* ctxt, const std::string& name) {
    CreateGraphCommand* cmd = new CreateGraphCommand(name);
    cmd->registerCmd(ctxt);
    return cmd;
}

// ListGraphCommand

ListGraphCommand* ListGraphCommand::create(ASTContext* ctxt) {
    ListGraphCommand* cmd = new ListGraphCommand();
    cmd->registerCmd(ctxt);
    return cmd;
}

// LoadGraphCommand

LoadGraphCommand::LoadGraphCommand(const std::string& fileName)
    :_fileName(fileName)
{
}

LoadGraphCommand::LoadGraphCommand(const std::string& fileName, const std::string& graphName)
    :_fileName(fileName),
    _graphName(graphName)
{
}

LoadGraphCommand* LoadGraphCommand::create(ASTContext* ctxt, const std::string& name) {
    LoadGraphCommand* cmd = new LoadGraphCommand(name);
    cmd->registerCmd(ctxt);
    return cmd;
}

LoadGraphCommand* LoadGraphCommand::create(ASTContext* ctxt, const std::string& name, const std::string& graphName) {
    LoadGraphCommand* cmd = new LoadGraphCommand(name, graphName);
    cmd->registerCmd(ctxt);
    return cmd;
}

// ExplainCommand

ExplainCommand::ExplainCommand(QueryCommand* query)
    : _query(query)
{
}

ExplainCommand* ExplainCommand::create(ASTContext* ctxt, QueryCommand* query) {
    ExplainCommand* cmd = new ExplainCommand(query);
    cmd->registerCmd(ctxt);
    return cmd;
}

// HistoryCommand

HistoryCommand::HistoryCommand()
{
}

HistoryCommand* HistoryCommand::create(ASTContext* ctxt) {
    HistoryCommand* cmd = new HistoryCommand;
    cmd->registerCmd(ctxt);
    return cmd;
}

// ChangeCommand

ChangeCommand::ChangeCommand(ChangeOpType type)
    : _type(type)
{
}

ChangeCommand* ChangeCommand::create(ASTContext* ctxt, ChangeOpType type) {
    ChangeCommand* cmd = new ChangeCommand(type);
    cmd->registerCmd(ctxt);
    return cmd;
}

// CallCommand

CallCommand::CallCommand(Type type)
    :_type(type)
{
}

CallCommand* CallCommand::create(ASTContext* ctxt, Type type) {
    CallCommand* cmd = new CallCommand(type);
    cmd->registerCmd(ctxt);
    return cmd;
}

S3ConnectCommand::S3ConnectCommand(std::string& accessId, std::string& secretKey, std::string& region)
    :_accessId(accessId),
    _secretKey(secretKey),
    _region(region)
{
}

S3ConnectCommand* S3ConnectCommand::create(ASTContext* ctxt, std::string& accessId, std::string& secretKey, std::string& region) {
    S3ConnectCommand* cmd = new S3ConnectCommand(accessId, secretKey, region);
    cmd->registerCmd(ctxt);
    return cmd;
}

S3ConnectCommand* S3ConnectCommand::create(ASTContext* ctxt) {
    S3ConnectCommand* cmd = new S3ConnectCommand();
    cmd->registerCmd(ctxt);
    return cmd;
}

S3TransferCommand::S3TransferCommand(Dir transferDir,const std::string& s3URL, const std::string& localDir) 
    :_transferDir(transferDir),
    _s3URL(s3URL),
    _localDir(localDir)
{
}

S3TransferCommand* S3TransferCommand::create(ASTContext* ctxt, Dir transferDir, const std::string& s3URL, const std::string& localDir) {
    S3TransferCommand* cmd = new S3TransferCommand(transferDir, s3URL, localDir);
    cmd->registerCmd(ctxt);
    return cmd;
}
