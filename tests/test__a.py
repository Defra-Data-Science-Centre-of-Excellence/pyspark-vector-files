"""Deal with globbing both for standard and zipped files.

where multiple zips = ok, lots of files in single zip is not possible
Check the bit before the extension for an asterix - if it comes after - no deal

If after globbing a sequence of strings/paths are returned, then initiate building
ways to handle multiple...

Add other vsi types for different protocols - dictionary of extensions/vsi key/value
(handle None case)
Check for what comes after the . in the string and look it up in the dictionary
Don't worry about s3

"/path/to/*.ext" -> ("/path/to/file.ext", ...)
"/path/to/*.zip" -> ("/vsizip/path/to/file.zip", ...)
"https://path/to/*.ext" -> Error

22/11/2022:

- Expand the prefixer to handle other prefixes
- How does this fit in with the single path case?

Think about....
- need some logic in the prefixing how to introduce other extensions e.g. tar
- whats the pythonic way of doing a case statement? (homework)


25/11/2022:

- Continue adding tests for all tar/gz combinations (renamed compressed to zipped)

"""
