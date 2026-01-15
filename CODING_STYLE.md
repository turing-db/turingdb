# TuringDB Project Coding Style Guidelines

## General & meta-rules

**Simplicity** 
Try to write for each task the simplest code that satisfies the requirements of the task. 
Do not write complex code just for showing off your technical capabilities. 
Most of the time the simplest and clearest code is the most performant, because its time and space behavior is the easiest to understand and to ensure
Think of the reader, of the person that will your code after you. It can be the reviewer, your peers or future you in a few months.

**Make the code breathe**
Insert blank lines in the implementation of a method to separate the steps or phases of an algorithm.
Use blank lines to facilitate understanding. We are not limited by the vertical size of the code.

**Make your code beautiful and cultivate your aesthetics sense**
Try to make your code elegant, beautiful, clear, whenever possible.

**Opinion of last resort**
Remy Boutonnet as the main designer and maintainer of TuringDB provides the opinion of last resort on any code review, PR or element of style or C++ for turingdb.


## Formatting

### Line length

We must have a maximum line length of 90 characters, with the objective to keep lines short and as readable as possible, with an ideal length around 80 characters. 
This is quite convenient to have 80-90 characters lines when we want to display many files side-by-side in vim on a wide screen.

### Indentation

We use 4 spaces indentation with spaces only. We do not use tabs.

### Includes

We use the #include \<stdlib.h\> style of standard includes, not #include \<cstdlib\>.

Always follow the preferred order of includes in turingdb:
* First the header file of the current class being implemented if that is the case
* Standard library headers
* External libraries, such as spdlog
* Then: an Outer-inner order relative to the current class, followed by utilities
** Important turingdb header files
** Header files of your current component
** Utility classes, assert, exception classes

Put the current header file first and put an empty line afterwards, before the remaining includes.
Include directives should be grouped into different paragraphs to facilitate reading. 
Utility includes should be written last and put in their own paragraph, and not mixed with other includes.

### Brackets

We put brackets { on the same line for a control flow construct such as if, for, while, with a space between the parenthesis and the opening bracket:
```cpp
if (somecond) {
    // Do something
} else {
   // Do something else
}

if (cond1) {
} else if (cond2) {
} else {
}

for (const auto& entry : _records) {
    // Do something
}

while (cond) {
}

do {
} while (cond);
```

Constructors are the only place where the opening bracket must be on the next line:
```cpp
class MyClass {
public:
    MyClass()
        : _member1("hello"),
        _member2(42)
    {
    }
};

MyClass::MyClass()
    : _member1("hello"),
    _member2(42)
{
}
```

Destructors are formatted line any other method with the opening bracket on the same line:
```cpp
MyClass::~MyClass() {
}
```

Functions and methods generally have opening brackets on the same line:
```cpp
void MyClass::myFunc() {
}
```

## Namespaces

1. Never do using namespace in header files. This is a horribly bad practice because it contaminates all the cpp files that are including the header file.
2. Never do using namespace std. We like to explicitly show that something is a standard container.
3. We create namespaces with care and when it makes sense, such as for things as important as db or Log. Never do it on your own, ask around you if it makes sense.

## Classes

### Class formatting

Example of the preferred way of formatting a class:
```cpp
class MyClass {
public:
	using Nodes = std::vector<MyClass*>;

	MyClass(MyClass* parent);
	~MyClass();

	const Nodes& nodes() const { return _nodes; }

private:
	MyClass* _parent {nullptr};
	Nodes _nodes;

	void myPrivateFunc();
};
```

1. The opening bracket { is on the same line as the class name.
2. The public and private keywords are at the same level of indentation as the class keyword.
3. Class members are indented with 4 spaces.
4. A blank line separates the public section from the private section.
5. The private section lists first the private members and then the private methods.
6. Private member variables begin with an underscore.
7. Method names follow the lower camelcase style, such as myPrivateFunc.
8. Never write big implementation code in header files. This results in bloated code and longer compile times.
9. Constructors and destructors are only declared in the header file but never implemented in headers for most classes. This can be tolerated for small classes with only POD data and no STL containers.
10. Very short methods can be implemented in header files for performance sake, usually getters and setters. Never do true algorithmic work in header files.
11. Never call methods from complex standard containers such as std::map or std::unordered_map in header files.
12. Never have public member variables.
13. Always initialize member pointer variables to nullptr in the class declaration, even if the constructor initialize them later. This is to avoid subtle errors due to typos or mistakes.
14. Never do work in constructors.
15. Destructors should be implemented in a cpp file whenever the class has non-trivial members, such as any STL container or classes that themselves have a non-trivial destructor.
16. Use your esthetic sense and do not add any unnecessary clutter.

### Constructors

**Never do work in constructors**

Constructors can not fail and there is no possibility of returning an error in a constructor, so we must only do the most trivial operations in a constructor, which is usually only setting the value of the private members of the class from the constructor arguments.

**Constructor formatting**

The typical formatting of a constructor is:
```cpp
MyClass::MyClass(ArgType* arg1, ArgType* arg2)
	: _attrA(arg1),
	_attrB(arg2)
{
}
```
- Brackets { } are put on the next line for constructors
- If the constructor argument line is short, put constructor arguments on the same line
- If the constructor argument line is long, break it into several lines
- Attribute initialisation is done on the line following the argument line
- The constructor body is ideally empty
- Each new member initialisation is aligned with the ":" character

## Function arguments

When a function has few arguments and they fit well on one line, they can be put one after the other on the same line:
```cpp
void MyClass::myFunction(Arg1* arg1, Arg2* arg2, Arg3* arg3) {
}
```

When a function has more than 3 arguments or they don't fit well on one line, the arguments have to be broken down
and put each on their own line. Each argument must then be correctly aligned one below the other:
```cpp
void MyClass::myFunction(Arg1* arg1,
                         Arg2* arg2,
						 Arg3* arg3,
						 HugeAndEnormousClass* arg4,
					     Arg5* arg5) {
}
```

## Function result type

Do not return non-trivial classes as a function return value. Do not return STL containers.
Do not count on RVO (Return Value Optimisation), assume that it does not exist.
Prefer to fill or pass results of functions by a modifiable reference in case of an STL container
or my passing a pointer to your class.

Do not return strings, this is very ugly.

## Pointers and references

Don't be afraid of pointers.

### Formatting

The star symbol of a pointer is always put close to the type without space:
```cpp
void func(Type* obj) {
}
```

### Argument-passing style
The primary argument-passing style in turingdb should be good old pointers, not references:
```cpp
void MyClass::myFunction(Arg1* arg1, Arg2* arg2, Arg3* arg3) {
}
```

References are reserved for passing STL containers:
```cpp
void MyClass::myFunction(Arg1* arg1, const std::string& myStr) {
	// Do something
}
```

Use a const reference whenever possible, to pass an STL structure that will not be modified:
```cpp
void MyClass::myFunction(Arg1* arg1, const std::string& myStr) {
	// Do something
}
```

When an STL container has to be filled up or modified or "returned", don't return it as a result of the function
but pass it by modifiable reference to the function:
```cpp
void MyClass::myFunction(Arg1* arg1, std::vector<double>& result) {
	// Do something
}
```

Do not mix passing styles.
Never pass smart pointers as function arguments.

### Ownership

The primary type of pointer in turingdb is the good old pointer: MyType* mytype

std::uniqueçptr can be used to denote an "owning" pointer: that pointer is the unique place owning the existence of an object.

Generally speaking, the meaning of pointers if the following:
* Good old pointer: non-owning pointer, unless otherwise specified
* Unique pointer: owning pointer 

Pay great attention to the ownership of your objects. In TuringDB we usually don't create and pass heap-allocated objects
freely as argument from function to function. Think first of the ownership structure, what class and which area of the codebase
should own the objects of your new class. Have a clear and unique owner of these objects. If it is hard or impossible
to manage ownership using good old pointers and new, or std::unique_ptr, it means that the structure of ownership is wrong.
Unique pointers should not be used to pass owned objects around from function to function using std::move, but just
as a shorthand to replace new and delete.

### Smart pointers

The only allowed type of smart pointer is std::unique_ptr.
Do not use std::shared_ptr as it reveals almost always a fault of the infrastructure.

## Const

We attach a lot of importance to const-correctness.
Always use const whenever possible, unless it adds mindless clutter.

### Const for arguments

Use const for pointers and references whenever they will never be modified or are not supposed to be modified.
```cpp
void myAlgorithm(std::vector<NodeID>& result, const std:::vector<NodeID>& seed, const AlgoConfig* config) {
}
```

### Const for local variables and intermediate results

Use const whenever possible for the local variables of a function and to denote the intermediate results of an algorithm.
```cpp
void myAlgorithm(std::vector<NodeID>& result, const std:::vector<NodeID>& seed, const AlgoConfig* config) {
	const double scorePhase1 = computePhase1(seed, config);
	
	// Do something...

	const double scorePhase2 = computePhase2(seed, config);

	return double scoreFinal = std::max(scorePhase1, scorePhase2);
}
```

## Initialisation

Always initialize local variables to a default value if necessary.
Always initialize class members to a default value, especially pointers.

Class member initialisation style:
```cpp
class MyClass {
public:
	// Something..

private:
	MyData* _data {nullptr};
};
```
Always use the brackets {} initialisation style for class members.
The brackets must be separated from the variable name by a space, such as:
```cpp
struct MyStruct {
    MyData* myData {nullptr};
};
```

Always use the assignment style of initialisation for local variables inside functions:
```cpp
void myFunc() {
	// Do something..

	MyData* data = nullptr;
	for (size_t i = 0; i < steps; i++) {
		data = computeStep(data);
	}
}
```

The preferred style for new is with an assignment:
```cpp
MyData* data = new MyData();
```

## Enum

Prefer enum class almost always.
Prefer to leave a comma at the end of the last alternative of an enum or enum class.

## Assertions

Use assertions in turingdb to ensure that a critical property of the code is valid at a given point,
to check conditions without which the code can not absolutely proceed.

Use only the bioassert API defined in BioAssert.h for assertions in turingdb.
We decided that bioassert raises an exception if the assertion is not valid.
Please do not catch that exception yourself. It is only try-catched in QueryInterpreter so that a given query does not crash the entire server.

## Exceptions

Exceptions are used and allowed in turingdb.
Prefer raising an exception by opposition to returning a boolean or an error code.

TuringDB already has a quite extensive set of exception classes associated to different stages of query processing, 
such as AnalyzeException, PipelineException...etc.

All exceptions in turingdb must derive from TuringException.

Only old code in turingdb still uses booleans to indicate errors. This code have to be adapted over time.

## Concepts

You can use C++ concepts in turingdb but please use them parsimoniously. The overall goal is to not add syntactical noise and clutter to the code.

## Move semantics and rvalue references

Pass data by pointer or reference, const reference or result reference, for consistency sake.
Please do not use RVO, move semantics or anything relying on rvalue references and other related mechanisms in C++.

## Switch formatting

Switch and cases are formatted as follows:
```cpp
switch (myCond) {
    case Value1:
        // Do something
    break;

    case Value2:
        // Do something
    break;
}
```

The break statements are aligned with the case statements.
Each case must have a break statement.
Be careful of using a default case as it can hide future cases that will need to be handled, such that for switch on an enum value.

Use a scoped block of statements with brackets if you start to have more than one statement in each case or if you need to declare variables
local to each case.
```cpp
switch (myCond) {
    case Value1: {
        const MyData* data = getMyDataFromSource1();
        compute1(data);
    }
    break;

    case Value2: {
        const MyData* data = getMyDataFromSource2();
        compute2(data);
    }
    break;
}
```
