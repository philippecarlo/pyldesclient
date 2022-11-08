Define Interface for the CLient
===
Idea is to setup something stateful in a directory using a file-based DB (here SQLite).
 - onboad a new collection using a view or other entrypoint?
   cli command: ldes sync <url> as <alias> poll <seconds>
   cli command: ldes sync <url> as <alias> subscribe
 - provide action hooks for new collections/members
   --> backend stuff
   --> built-in support for emitting to ... redis, kafka, ... ?
 - delete an existing collection
   cli command: ldes remove <collection-id> | <alias>
 - collection status management
   - list all collections
     cli command: ldes list
   - get the status of a collection that is syncing
     cli command: ldes status <collection-id> | <alias>
   - pause/resume syncing for a collection
     ldes pause|resume <collection-id> | <alias>
   - 