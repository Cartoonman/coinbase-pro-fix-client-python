# coinbase-pro-fix-client-python
FIX Client Implementation for Coinbase Pro, written in Python.

# yafi
Yet Another FIX Interface

## TODOS
* Refactor handling of tags in messages so we don't have to keep doing [0] for singular tags. (how to deal with repeating tags)
* Cleanup and properly scope the FIX Implementation component of this client (should be barebones, limit it's scope)
* Consider using QuickFIX's xml definitions for FIX 4.2 with a modified version for Coinbase.
* * Potentially move from literal 'x' to ENUM based calls for tag and message types. TBD. 
* Actually implement heartbeats properly
* Clean up module namespacing
* Add licensing and proper attributions to codebase.
* Docstrings for all functions
* Add mypy
* Tests