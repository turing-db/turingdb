## Check that there are no spurious useless blanklines

* No 2 or more consecutive blank lines
* No blank line immediately following the opening brace of a function, class or method
* A trailing blank line at the end of a file is fine and not a violation

Example:
```
void myFunc() {
                          <-- this blank line is a violation
    return 42;
}

class MyClass {
                          <-- this blank line is a violation
public:
    void hello();
};
```

## Check initialization style

* Classes and struct member variables must be initialized with the bracket {defaultValue}
initialisation style. STL iterators are exempt from this rule as they are value-initialized
by their default constructor

```
class MyClass {
private:
    MyPointer* _ptr {nullptr};
};
```

* All member variables of a pointer type must be default initialized to null at the declaration site

```
class MyClass {
private:
    MyPointer* _ptr {nullptr};
};
```

* Local variables inside a function or method must be initialised using the assignment style
```
void myFunc() {
    MyPointer* ptr = nullptr;
}
```

## Check for const correctness

* Every argument or variable of pointer or reference type that can be const has to be const
* Every local variable of pointer or reference type that can be const has to be const
* Every local variable that is never modified and can not be modified inside a function has to be const
* This rule does not apply to the return type of functions or methods

## Member variables names

All names of member variables of classes or structs must start with an underscore.

Example:
```
class MyClass {
private:
    MyOtherClass _otherClass;
};
```

## Check that there are no using namespace in header files

## Any class with non-trivial member variables should have constructors and destructors in cpp file

Any class or struct with member variables that are non-POD types or considered as non-trivial,
such as STL data structures, or classes that are themselves non-trivial, must have their constructors
and destructors defined in a cpp file.

## Check constructor and destructor formatting

* Constructors are formatted with the brackets positioned on the following line
* Destructors are formatted like any other method, with the opening bracket on the first line

```
MyClass::MyClass()
{
}

MyClass::~MyClass() {
}
```

## Check no use of shared pointers

Shared pointers are forbidden in the code base.

## Do not pass objects via unique_ptr unless special cases

The preferred way of managing ownership is to register objects into context or manager objects
and use a static create method where an object registers itself in its owner, via the mutual friend relationship.

Unless good reason, objects should not be passed around as unique pointers.
Unique pointers are tolerated for the construction of the big components of the db at startup,
but these are few and well-known. We should not have chains of function calls passing unique pointers to each other.

## Do not return non-trivial objects and do not return STL data structures

* Do not return stl data structures, pass them by result reference
* In particular do not return string, unless it is a small string constant
* Small, lightweight result or status types (status codes, error wrappers, expected-style results) may be returned by value

## Do not use noexcept

## Do not use [[nodiscard]] everywhere in a class

The use of nodiscard everywhere is just adding clutter to the code. 

## Check that meaningful variable names are given

We need to have meaningful variable names.
The judgement is subjective. But for example, naming variables "Writer w" is not good.
Short names like n, m, i..etc are tolerable depending on the context, when it is programming custom to do so.

## Check that ternary operator is only used for expressions

Do not use ternary operator for constructs that are statements.
Always use if statements for statements.

## Check that brackets are always used for if statements

Do not accept if statements without brackets, like:
```
if (expr)
    doSomething();
```

## Line length is not strict

Coding style say that a 90 characters line length is preferable but that's not a religion.
The most important principle is esthetics of the code. It is acceptable to have a longer line size
if the code is more beautiful.

## Check that the primary passing mechanism is good old pointers

Do not pass any class or struct as reference unless it is an STL data structure or a callable type such as `std::function`.
Small, lightweight status or result structs may also be passed by const reference when it is the natural style.
STL data structures include `std::string`, `std::string_view`, `std::vector`, `std::map`, `std::optional` and other standard library types.
Good old pointers are the primary passing mechanism.
