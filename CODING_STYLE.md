# TuringDB Project Coding Style Guidelines

## Philosophy

We see software engineering as being the art and practice of creating software.
It is a practice that has a long history, dating back from the 1940s in England,
and the foundational work of Alan Turing who brought the message of computer science to the world.

Software engineering is the conventional term of the field but software craftsmanship would be closer to the actual practice and to our vision. 
We very much think that we do not code and that we are not coding or just hacking around. 
For the subculture and the conceptions behind it, programming is the proper word. 
We prefer to say that we write or create software, for the level of respect and reverence that we have and that it inspires towards this activity and its practitioners.

**Software as a creative act**

We believe that writing software is a fundamentally creative activity of mankind. 
It is about taking into account requirements, constraints and properties of what must be done, mixing them in the creative cauldron of one's mind and firing up the imaginary. 
Great software emerges from ideas ultimately coming from intuition out of the imaginary.
These great ideas of interest are often charged with an aesthetic sentiment or have an aesthetic quality in themselves.

The behavior of a piece of software, its properties, both functional and non-functional, can be analyzed, described, formalized, and studied in various ways by different fields of computer science. 
Software at its most fundamental level, as the actual realization by machine of a program, of an abstract computational entity, is studied by the theory of computation.
Software engineering research is studying the professional practice of programming and produced a great quantity of recommendations and methodologies on how to produce good software. 

The production of software can be investigated externally and after the act, by the methods of experimental science. But the actual act itself of an individual writing software is very much a creative act emerging from the human imaginary.

The best software ideas are produced by intuition, rather than by the means of explicit and conscious logical deductive processes.
We give them a justification after they have emerged. This justification may be technical, practical or theoretical. 
This concerns architectural ideas, visions of components and systems interacting elegantly with each other.

**The importance of imagination, creativity and diversity**

Even if computer science is the science of computational entities and software, the actual act of producing a piece of software by a human, 
that is the human experience of writing software, is a creative act, rather than a purely step-by-step process that could be completely prescribed by some kind of engineering methodology. 

And that creation comes from the imaginary, that is the imagination of a living human being.
Thus we place a great importance and respect for the people writing software, as they are not and can not be just the executants of some methodology or engineering recipe.
Our imaginary is populated by what we experience, what we live, see, read, smell, enjoy.
It is filled by our own life experiences in all domains of life, our backgrounds, the personal and the collective. 
Thus software ideas originates from a unique quality present in each human individual.

So to create the best software, it is of great importance to have the broadest and deepest range of imaginary space, in the people and the teams writing software.
Thus diversity of the people making software, is of the essence to make great software.
And on the personal side, we only grow and become better persons because of it.

## C++

### General & meta-rules

**Simplicity** 
Try to write for each task the simplest code that satisfies the requirements of the task. 
Do not write complex code just for showing off your technical capabilities. 
Most of the time the simplest and clearest code is the most performant, because its time and space behavior is the easiest to understand and to ensure
Think of the reader, of the person that will your code after you. It can be the reviewer, your peers or future you in a few months.

**Make the code breathe**
Insert blank lines in the implementation of a method to separate the steps or phases of an algorithm.
Use blank lines to facilitate understanding. We are not limited by the vertical size of the code.

**Make your code beautiful and cultivate your esthetics sense**
Try to make your code elegant, beautiful, clear, whenever possible.

**Opinion of last resort**
Remy Boutonnet as the main designer and maintainer of TuringDB provides the opinion of last resort on any code review, PR or element of style or C++ for turingdb.


### Formatting

#### Line length

We must have a maximum line length of 90 characters, with the objective to keep lines short and as readable as possible, with an ideal length around 80 characters. 
This is quite convenient to have 80-90 characters lines when we want to display many files side-by-side in vim on a wide screen.

#### Indentation

We use 4 spaces indentation with spaces only. We do not use tabs.

#### Includes

We use the #include <stdlib.h> style of standard includes, not #include <cstdlib>.

Always follow the preferred order of includes in turingdb:
* First the header file of the current class being implemented if that is the case
* Standard library headers
* External libraries, such as spdlog
* Then: an Outer-inner order relative to the current class, followed by utilities
** Important turingdb header files
** Header files of your current component
** Utility classes, assert, exception classes

Put the current header file first and put an empty line afterwards, before the remaining includes.
Include directives should be groupes into different paragraphs to facilitate reading. 
Utility includes should be written last and put in their own paragraph, and not mixed with other includes.

#### Brackets

We put brackets { on the same line for a control flow construct such as if, for, while, with a space between the parenthesis and the opening bracket:
```
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
```
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
```
MyClass::~MyClass() {
}
```

Functions and methods generally have opening brackets on the same line:
```
void MyClass::myFunc() {
}
```

### Namespaces

1. Never do using namespace in header files. This is a horribly bad practice because it contaminates all the cpp files that are including the header file.
2. Never do using namespace std. We like to explicitly show that something is a standard container.
3. We create namespaces with care and when it makes sense, such as for things as important as db or Log. Never do it on your own, ask around you if it makes sense.

### Classes

#### Class formatting

Example of the preferred way of formatting a class:
```
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

#### Constructors

**Never do work in constructors**

Constructors can not fail and there is no possibility of returning an error in a constructor, so we must only do the most trivial operations in a constructor, which is usually only setting the value of the private members of the class from the constructor arguments.

**Constructor formatting**

The typical formatting of a constructor is:
```
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

### Function arguments

When a function has few arguments and they fit well on one line, they can be put one after the other on the same line:
```
void MyClass::myFunction(Arg1* arg1, Arg2* arg2, Arg3* arg3) {
}
```

When a function has more than 3 arguments or they don't fit well on one line, the arguments have to be broken down
and put each on their own line. Each argument must then be correctly aligned one below the other:
```
void MyClass::myFunction(Arg1* arg1,
                         Arg2* arg2,
						 Arg3* arg3,
						 HugeAndEnormousClass* arg4,
					     Arg5* arg5) {
}
```

### Function result type

Do not return non-trivial classes as a function return value. Do not return STL containers.
Do not count on RVO (Return Value Optimisation), assume that it does not exist.
Prefer to fill or pass results of functions by a modifiable reference in case of an STL container
or my passing a pointer to your class.

Do not return strings, this is very ugly.

### Pointers and references

Don't be afraid of pointers.

#### Formatting

The star symbol of a pointer is always put close to the type without space:
```
void func(Type* obj) {
}
```

#### Argument-passing style
The primary argument-passing style in turingdb should be good old pointers, not references:
```
void MyClass::myFunction(Arg1* arg1, Arg2* arg2, Arg3* arg3) {
}
```

References are reserved for passing STL containers:
```
void MyClass::myFunction(Arg1* arg1, const std::string& myStr) {
	// Do something
}
```

Use a const reference whenever possible, to pass an STL structure that will not be modified:
```
void MyClass::myFunction(Arg1* arg1, const std::string& myStr) {
	// Do something
}
```

When an STL container has to be filled up or modified or "returned", don't return it as a result of the function
but pass it by modifiable reference to the function:
```
void MyClass::myFunction(Arg1* arg1, std::vector<double>& result) {
	// Do something
}
```

Do not mix passing styles.
Never pass smart pointers as function arguments.

#### Ownership

The primary type of pointer in turingdb is the good old pointer: MyType* mytype

std::uniqueçptr can be used to denote an "owning" pointer: that pointer is the unique place owning the existence of an object.

Generally speaking, the meaning of pointers if the following:
* Good old pointer: non-owning pointer, unless otherwise specified
* Unique pointer: owning pointer 

#### Smart pointers

The only allowed type of smart pointer is std::unique_ptr.
Do not use std::shared_ptr as it reveals almost always a fault of the infrastructure.

### Const

We attach a lot of importance to const-correctness.
Always use const whenever possible, unless it adds mindless clutter.

#### Const for arguments

Use const for pointers and references whenever they will never be modified or are not supposed to be modified.
```
void myAlgorithm(std::vector<NodeID>& result, const std:::vector<NodeID>& seed, const AlgoConfig* config) {
}
```

#### Const for local variables and intermediate results

Use const whenever possible for the local variables of a function and to denote the intermediate results of an algorithm.
```
void myAlgorithm(std::vector<NodeID>& result, const std:::vector<NodeID>& seed, const AlgoConfig* config) {
	const double scorePhase1 = computePhase1(seed, config);
	
	// Do something...

	const double scorePhase2 = computePhase2(seed, config);

	return double scoreFinal = std::max(scorePhase1, scorePhase2);
}
```

### Initialisation

Always initialize local variables to a default value if necessary.
Always initialize class members to a default value, especially pointers.

Class member initialisation style:
```
class MyClass {
public:
	// Something..

private:
	MyData* _data {nullptr};
};
```
Always use the brackets {} initialisation style for class members.

Always use the assignment style of initialisation for local variables inside functions:
```
void myFunc() {
	// Do something..

	MyData* data = nullptr;
	for (size_t i = 0; i < steps; i++) {
		data = computeStep(data);
	}
}
```

The preferred style for new is with an assignment:
```
MyData* data = new MyData();
```

### Enum

Prefer enum class almost always.
Prefer to leave a comma at the end of the last alternative of an enum or enum class.

### Assertions

Use assertions in turingdb to ensure that a critical property of the code is valid at a given point,
to check conditions without which the code can not absolutely proceed.

Use only the bioassert API defined in BioAssert.h for assertions in turingdb.
We decided that bioassert raises an exception if the assertion is not valid.
Please do not catch that exception yourself. It is only try-catched in QueryInterpreter so that a given query does not crash the entire server.

### Exceptions

Exceptions are used and allowed in turingdb.
Prefer raising an exception by opposition to returning a boolean or an error code.

TuringDB already has a quite extensive set of exception classes associated to different stages of query processing, 
such as AnalyzeException, PipelineException...etc.

All exceptions in turingdb must derive from TuringException.

Only old code in turingdb still uses booleans to indicate errors. This code have to be adapted over time.

### Concepts

You can use C++ concepts in turingdb but please use them parsimoniously. The overall goal is to not add syntactical noise and clutter to the code.

### Move semantics and rvalue references

Pass data by pointer or reference, const reference or result reference, for consistency sake.
Please do not use RVO, move semantics or anything relying on rvalue references and other related mechanisms in C++.
