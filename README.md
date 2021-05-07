# KeyValueSqlite

Super easy drop in replacement for python dictionary, which stores
it's key-value to an sqlite database.

This datastructure is not going to win any performance races, but it
is super simple to use with just a few lines of code. This is a great
option for one of those small web apps which doesn't have enough load
to justify mysql or postgres, or a file that will be used by multiple
processes, or to store a file that can't be corrupted during a power
failure.

When fetching large amounts of data try to use get_many() or dict_range().


# TODO:
  * Make read access use a shared lock rather than an exclusive lock.