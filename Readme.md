REPO-DS9 (tentative name)
==============

A successor to repo-ng (as DS9 is a successor to TNG...)    

Implements the repo-ng protocol, but stores data in MongoDB, and requires 
name-based schemata to insert data.

An example schema may look like:

    /ndn/edu/ucla/<username>/documents/<document_name>

The components enclosed in \<\> are treated as keys into the data store, so a
 query may be made to find all document names for a user, or all usernames who
 have a specific document, etc.


Supported Syntax
----------------

This database will supports a GQL-like 'SELECT' statement:

    SELECT *| <key name list>
        [FROM identifier]
        [WHERE <condition> [AND <condition>]]
        [LIMIT <count>]
        [OFFSET <count>]

    <key name list> := <key name> [, <key name> ...]
    <condition> := <key name> = <value>
    <identifier> := [A-Za-z0-9_.]

The `FROM` clause is currently ignored, and only equality is currently supported
in conditions. Furthermore, the `<key name list>` is ignored and treated 
as `*`.

Notice also that `LIMIT` and `OFFSET` are separate clauses - in particular, 
the `LIMIT <offset>, <count>` syntax is not supported. The `LIMIT` and `OFFSET` clauses are currently ignored.

All key names are *case-sensitive*; all keywords (SELECT, FROM, WHERE,
 AND, LIMIT, OFFSET) are *case-insensitive*.

###Not yet implemented:
*   Use 'FROM' clause to specify which schema is being referenced
*   Support more operators in conditions (especially for timestamps)
*   Add type fields to schema (so we know which are datetime/comparable/binary)

Example Schema
--------------

This is based on data captured by a Building Management System (BMS) installed 
at UCLA. As such, the data is not currently available to the general public. 
The schema is defined as

    /ndn/ucla.edu/bms/<building>/data/<room>/electrical/panel/<panel_name>/<quantity>/<quantity_type>/

This allows for queries such as:
*   `SELECT * FROM bms WHERE panel_name=X AND quantity=voltage`
*   `SELECT * WHERE room=101A AND quantity=current LIMIT 10 OFFSET 20`
*   `SELECT room, panel_name FROM bms WHERE building=building1`

(Remember that the `FROM` clause currently has no effect.)

Notes On Storing/Querying Data
------------------------------

The bms\_client.py included here goes through an extra step of downloading 
keys, since the BMS data is encrypted. In your own implementation, you will
typically do processing as soon as the data is received from the repo.

The bms\_ping.py script periodically pokes the repo to insert certain data. 
In practice, a data publisher will do this itself when there is data to be put
in the repo.

A more typical repo example will come soon.

Repo Commands/Protocol
----------------------

A full documentation of the repo-ng protocol can be found on the
[redmine page](http://redmine.named-data.net/projects/repo-ng/wiki). Currently,
repo-ds9 only supports the insert command.
