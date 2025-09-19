# Prefix Tree (Trie) implementation
> Cyrus Knopf 1/7/25

To build:
`build` (standard alias)

Usage:

Binary in `.../turingdb/build/build_package/samples/prefix-tree`


To interact with the trie on the command line, after building, run:
```
./prefix-tree --cli
```

To test out the preprocessor on an input string, run:
```
./prefix-tree --ppx <YOUR_STRING>
```


To benchmark the trie against a standard STL hash implementation, run:
```
./prefix-tree --bm <PATH_TO_DICTIONARY> <PATH_TO_QUERIES>
```
> Run `setup.sh` in `src` to get sample data and a sampling script in your build directory.

The dictionary is the set of words which will be inserted into the data structure, whilst the query file is the set of words that will be called with `find`.

`sample.sh` is provided to generate queries from a dictionary. It creates `n` samples, with `n/2` being from the provided dictionary, `n/4` being randomly truncated samples from the dictionary (to mimic partial matching in the trie), and `n/4` being completely random strings.

`./sample.sh <NUM_QUERIES> <FILE_TO_SAMPLE_FROM> <FILE_TO_OUTPUT_SAMPLES>`
