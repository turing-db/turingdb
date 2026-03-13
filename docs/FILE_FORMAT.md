# New Storage File Format

We describe in this specification a new format for turing graphs.

The objective is to store a TuringDB graph in one file if possible,
so that turingdb graphs just like SQLite database files can be copied other easily,
and for the esthetics of being self-contained and for decreasing the amount of system calls.

Another object pertaining to this file format in itself is to have more compact
disk storage for graphs than the first disk encoding format we have.

We want to use the ".turing" file extension for the TuringDB files of this new format.

In particular we want all the commits and branches of a given graph stored in that same file,
as well as all the dataparts and other metadata for that graph.

## Objectives and constraints

* All the dataparts and commits for a given graph are stored in the same file
* We want writers from different branches to be able to write in parallel to the file
* Parallelism and contention: 
** Contention 1: new page allocation in the file. But we could have a pool of free pages reserved per branch.
** Contention 2: creation of a new branch. No contention in the file for writing pages that are intra-branch.
* Writing commits and data parts inside a branch should be able to proceed in parallel over multiple writers provided that we have free pages and the branch is already created in the file.

Why there are no contention on data parts and why we don't need a central datapart directory in the file:
* We commits are created by inheriting data parts from their parents and creating new data parts
* So if we know the commit we are based on in memory, we know our dataparts and the datapart we created
* So no need for a central directory of all dataparts for all branches and all commits because each commit just knows on which dataparts it is based and can just reference them
* The fact of being a datapart in the file is just being a datapart frame referenced by one or more commits

## File structure

* Branch directory: lists each branch in some number of entries, can have many pages if a lot of branches.
* Each branch has a branch index that lists the commits in the branch. The branch index can have many pages if we have a long chain of commits in a branch. We assume one writer per branch so no contention to just append a commit entry in the branch index.
* The branch index consists of a header page and a number of pages.
* Each commit is described in a structure spanning possibly many pages called a frame. Commits are described by a number of sections. We don't want to limit today the number of shapes of sections in a given commit so a commit frame can span multiple pages.
* Each datapart is described by a frame spanning multiple pages. A datapart has sections for the various components of a datapart, such as NodeContainer, EdgeContainer..etc.

```
_____________________________________
Page 0: SUPERBLOCK

* File header 
* branch_dir (uint64_t): pointer to first branch directory page

--------------------------------------
Page 1 to BDir_end: BRANCH DIRECTORY PAGES

Goal: listing all branches in the graph

* branch_dir_magic (uint32_t)

Branch entries:
* branch_name_size (uint16_t)
* branch_name (uint8_t[]) not null terminated
* index_header_page (uint64_t): pointer to branch_index_header_page

Zero padding to page boundary
branch_dir_crc32 (uint32_t): last 4 bytes of a page, CRC-32 of all preceding bytes
-----------------------------------------
BRANCH INDEX HEADER PAGES

Goal: metadata about one branch

* branch_index_header_magic (uint32_t)
* head_commit (uint64_t): hash of the branch HEAD commit
* num_commits (uint64_t)
* index_start_page (uint64_t): pointer to first branch index page
* index_num_pages (uint64_t): number of branch index pages

Zero padding to page boundary
branch_index_header_crc32 (uint32_t): last 4 bytes of the page, CRC-32 of all preceding bytes
------------------------------------------
BRANCH INDEX PAGES

Goal: list all the commits of one branch

* branch_index_magic (uint32_t)

Commit entries:
* commit_hash (uint64_t)
* frame_start_page (uint64_t): first page of the commit frame
* frame_num_pages (uint64_t): number of pages of the commit frame
* parent_hash (uint64_t)
* timestamp (uint64_t)

Zero to page boundary
branch_index_crc32 (uint32_t): last 4 bytes of the page, CRC-32 of all preceding bytes
-------------------------------------------
COMMIT FRAME

Goal: consists of many possibly many pages describing a commit with sections

* commit_frame_magic (uint32_t)
* commit_frame_num_pages (uint64_t)
* commit_hash (uint64_t)
* parent_hash (uint64_t)
* timestamp (uint64_t)
* num_nodes (uint64_t)
* num_edges (uint64_t)

* section_num (uint16_t)
Section entries:
* section_tag (uint8_t)
* section_offset (uint64_t)
* section_size (uint64_t)

Sections:

0x10 DATAPARTS
* section_tag (uint8_t)
* dataparts_num (uint64_t)
Datapart entries:
* datapart_id (uint64_t)
* datapart_owner_hash (uint64_t)
* frame_start_page (uint64_t)
* frame_num_pages (uint64_t)

0x11 LABEL_MAP
* section_tag (uint8_t)
* label_num (uint64_t)
Label entries:
* label_id (uint64_t)
* label_name_size (uint16_t)
* label_name (uint8_t[])

0x12 LABEL_SET_MAP

0x13 EDGE_TYPE_MAP

0x14 PROPERTY_TYPE_MAP

0x15 JOURNAL

* commit_frame_end (uint32_t)
* commit_frame_crc32 (uint32_t)
-------------------------------------------
DATAPART FRAME

Goal: consists of possibly many pages describing the content of a datapart

* datapart_frame_magic (uint32_t)
* datapart_frame_num_pages (uint64_t)
* datapart_id (uint64_t)
* commit_creator_hash (uint64_t)

* section_num (uint16_t)
Section entries:
* section_tag (uint8_t)
* section_offset (uint64_t)
* section_size (uint64_t)

Sections:

0x20 NODE_CONTAINER
* section_tag (uint8_t)

0x21 EDGE_CONTAINER
* section_tag (uint8_t)

0x22 NODE_PROPERTIES
* section_tag (uint8_t)

0x23 EDGE_PROPERTIES
* section_tag (uint8_t)

0x24 EDGE_INDEXER
* section_tag (uint8_t)

0x25 NODE_PROPERTY_INDEXER
* section_tag (uint8_t)

0x26 EDGE_PROPERTY_INDEXER
* section_tag (uint8_t)

* datapart_frame_end (uint32_t)
* datapart_frame_crc32 (uint32_t)
-------------------------------------------
```
