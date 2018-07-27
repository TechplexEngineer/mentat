initSidebarItems({"fn":[["datoms","Return the set of datoms in the store, ordered by (e, a, v, tx), but not including any datoms of the form [... :db/txInstant ...]."],["datoms_after","Return the set of datoms in the store with transaction ID strictly greater than the given `tx`, ordered by (e, a, v, tx)."],["dump_sql_query","Execute the given `sql` query with the given `params` and format the results as a tab-and-newline formatted string suitable for debug printing."],["fulltext_values","Return the set of fulltext values in the store, ordered by rowid."],["tempids",""],["to_entid","Convert a numeric entid to an ident `Entid` if possible, otherwise a numeric `Entid`."],["transactions_after","Return the sequence of transactions in the store with transaction ID strictly greater than the given `tx`, ordered by (tx, e, a, v)."]],"struct":[["Datom","Represents a datom (assertion) in the store."],["Datoms","Represents a set of datoms (assertions) in the store."],["FulltextValues","Represents the fulltext values in the store."],["TempIds",""],["TestConn",""],["Transactions","Represents an ordered sequence of transactions in the store."]]});