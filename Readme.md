REPO-DS9 (tentative name)
--------

A successor to repo-ng (as DS9 is a successor to TNG...)
Implements the repo-ng protocol, but stores data in MongoDB, and requires name-based schemata to insert and query data.

An example schema may look like:

    /ndn/edu/ucla/\<username\>/documents/\<document_name\>

The components enclosed in \<\> are treated as keys into the data store
