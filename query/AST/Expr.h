#pragma once

#include "ExprConstraint.h"
#include "metadata/PropertyType.h"

#include <string>

namespace db {

class ASTContext;
class VarDecl;

class Expr {
public:
    friend ASTContext;

    enum Kind {
        EK_VAR_EXPR,
        EK_CONST_EXPR,
        EK_BIN_EXPR
    };

    virtual Kind getKind() const = 0;

protected:
    Expr() = default;
    virtual ~Expr();
    void postCreate(ASTContext* ctxt);
};

class VarExpr : public Expr {
public:
    static VarExpr* create(ASTContext* ctxt, const std::string& varName);

    Expr::Kind getKind() const override { return EK_VAR_EXPR; }

    const std::string& getName() const { return _varName; }

    VarDecl* getDecl() const { return _decl; }

    void setDecl(VarDecl* decl) { _decl = decl; }

private:
    const std::string _varName;
    VarDecl* _decl {nullptr};

    VarExpr(const std::string& varName);
    ~VarExpr();
};

class ExprConst : public Expr {
public:
    Expr::Kind getKind() const override { return EK_CONST_EXPR; }
    db::ValueType getType() const { return _type; }

protected: 
    db::ValueType _type {db::ValueType::Invalid};

    ExprConst(db::ValueType t):
        _type(t)
    {
    }
    virtual ~ExprConst();
};

class BoolExprConst : public ExprConst {
public:
    static BoolExprConst* create(ASTContext* ctxt, bool val);

    const db::CustomBool& getVal() const { return _val;};

private:
    db::CustomBool _val ;

    BoolExprConst(bool val)
        : ExprConst(db::ValueType::Bool),
        _val(val)
    {
    }

    ~BoolExprConst() = default;
};

class StringExprConst : public ExprConst {
public:
    static StringExprConst* create(ASTContext* ctxt, const std::string& val);

    const std::string& getVal() const { return _val;};
private:
    const std::string _val;

    StringExprConst(const std::string& val)
    : ExprConst(db::ValueType::String),
    _val(val)
    {
    }

    ~StringExprConst();
};

class UInt64ExprConst : public ExprConst {
public:
    static UInt64ExprConst* create(ASTContext* ctxt, uint64_t val);

    uint64_t getVal() const { return _val; };

private:
    uint64_t _val {0};

    UInt64ExprConst(uint64_t val)
        : ExprConst(db::ValueType::UInt64),
        _val(val)
    {
    };
    ~UInt64ExprConst() = default;
};

class Int64ExprConst : public ExprConst {
public:
    static Int64ExprConst* create(ASTContext* ctxt, int64_t val);

    int64_t getVal() const { return _val; };

private:
    int64_t _val {0};

    Int64ExprConst(int64_t val)
        : ExprConst(db::ValueType::Int64),
        _val(val) 
    {
    };
    ~Int64ExprConst() = default;
};

class DoubleExprConst : public ExprConst {
public:
    static DoubleExprConst* create(ASTContext* ctxt, double val);

    double getVal() const { return _val; };

private:
    double _val {0};
    DoubleExprConst(int64_t val)
        : ExprConst(db::ValueType::Double),
        _val(val)
    {
    };
    ~DoubleExprConst() = default;
};

template <SupportedType T>
struct PropertyTypeExprConst {};

template <> struct PropertyTypeExprConst<types::Int64> {
    using ExprConstType = Int64ExprConst;
};

template <> struct PropertyTypeExprConst<types::UInt64> {
    using ExprConstType = UInt64ExprConst;
};

template <> struct PropertyTypeExprConst<types::Double> {
    using ExprConstType = DoubleExprConst;
};

template <> struct PropertyTypeExprConst<types::String> {
    using ExprConstType = StringExprConst;
};

template <> struct PropertyTypeExprConst<types::Bool> {
    using ExprConstType = BoolExprConst;
};


class BinExpr : public Expr {
public:
    enum OpType {
        OP_EQUAL,
    };

    static BinExpr* create(ASTContext* ctxt, Expr* left, Expr* right, OpType opType);
   
    Expr::Kind getKind() const override { return EK_BIN_EXPR; }
    OpType getOpType() const { return _opType; }
    Expr* getLeftExpr() const { return _lexpr; }
    Expr* getRightExpr() const { return _rexpr; }

private:
    Expr* _lexpr {nullptr};
    Expr* _rexpr {nullptr} ;
    OpType _opType; 
    BinExpr(Expr* left, Expr* right, OpType _opType);
    ~BinExpr();


};
}
