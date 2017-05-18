# Purview

[![Build Status](https://secure.travis-ci.org/jzaleski/purview.png?branch=master)](http://travis-ci.org/jzaleski/purview)
[![Dependency Status](https://gemnasium.com/jzaleski/purview.png)](https://gemnasium.com/jzaleski/purview)

A framework designed to simplify data warehousing

## Installation

Add this line to your application's Gemfile:

    gem 'purview'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install purview

## Usage

Load the `MySQL` client (for `MSSQL` simply change 'mysql2' to 'tiny_tds'; for
`PostgreSQL` simply change 'mysql2' to 'pg' -- when using this gem in a JRuby
environment the 'jdbc/jtds', 'jdbc/mysql' and/or 'jdbc/postgres'library must be
installed/available)
```ruby
require 'mysql2'
```

Set the table-name (this can be anything, but it must exist)
```ruby
table_name = :users
```

Define the `Column(s)` (available column-types: `Boolean`, `CreatedTimestamp`,
`Date`, `Float`, `Id`, `Integer`, `Money`, `String`, `Text`, `Time`, `Timestamp`,
`UpdatedTimestamp` & `UUID` -- the `Id`, `CreatedTimestamp` & `UpdatedTimestamp`
columns are required for all `BaseSyncable` tables)
```ruby
id_column = Purview::Columns::Id.new(:id)
name_column = Purview::Columns::String.new(:name, :nullable => false)
email_column = Purview::Columns::String.new(:email, :nullable => false, :limit => 100)
created_at_column = Purview::Columns::CreatedTimestamp.new(:created_at)
updated_at_column = Purview::Columns::UpdatedTimestamp.new(:updated_at)

columns = [
  id_column,
  name_column,
  email_column,
  created_at_column,
  updated_at_column,
]
```

Define the `Indices` (availble index-types: `Composite` & `Simple`). By default
`Indices` will be added for the required column-types (`CreatedTimestamp` &
`UpdatedTimestamp`)
```ruby
indices = [Purview::Indices::Simple.new(email_column, :unique => true)]
```

Configure the `Puller` (available puller-types: `MSSQL`, `MySQL`, `PostgreSQL` &
`URI`)
```ruby
puller_opts = {:type => Purview::Pullers::URI, :uri => 'http://feed.test.com/users'}
```

Configure the `Parser` (available parser-types: `CSV`, `SQL` & `TSV`)
```ruby
parser_opts = {:type => Purview::Parsers::TSV}
```

Configure the `Loader` (for `PostgreSQL` simply change `MySQL` to `PostgreSQL`)
```ruby
loader_opts = {:type => Purview::Loaders::MySQL}
```

Combine all the configuration options and instantiate the `Table`
```ruby
table_opts = {
  :columns => columns,
  :indices => indices,
  :loader => loader_opts,
  :parser => parser_opts,
  :puller => puller_opts,
}

table = Purview::Tables::Raw.new(table_name, table_opts)
```

Set the database-name (this can be anything, but it must exist)
```ruby
database_name = :data_warehouse_raw
```

Combine all the configuration options and instantiate the `Database` (for
`PostgreSQL` simply change `MySQL` to `PostgreSQL`)
```ruby
database_opts = {:tables => [table]}

database = Purview::Databases::MySQL.new(database_name, database_opts)
```

Create the `Table` (in the DB). Recommended for testing purposes *only*. For
production environments you will likely want an external process to manage the
schema (for `PostgreSQL` simply change `Mysql2::Error` to `PG::DuplicateTable`,
for `SQLite` simply change `Mysql2::Error` to `SQLite3::SQLException`)
```ruby
begin
  database.create_table(table)
rescue Mysql2::Error
  # Swallow
end
```

Initialize the `Table` (in the DB). This process sets the `max_timestamp_pulled`
value in the `table_metadata` table and is used by the candidate `Table`
selection algorithm to determine which `Table` should be synchronized next (the
least recently synchronized `Table` will be selected). This value is also used
as the high-water mark for records pulled from its source. Unless a `timestamp`
is specified, as the second argument, the high-water mark will default to
`Time.now.utc`
```ruby
database.initialize_table(table)
```

Baseline the `Table`. This process will quickly get the state of the `Table` as
close to the current state as possible. This is generally useful when adding a
new `Table` to an existing schema (ideally this should be done while the `Table`
is disabled)
```ruby
database.baseline_table(table)
```

Enable the `Table` (in the DB). This process sets the `enabled_at` value in the
`table_metadata` table and is used by the candidate `Table` selection algorithm
to determine the pool of `Table(s)` available for synchronization (to remove a
`Table` from the pool simply execute `disable_table`)
```ruby
database.enable_table(table)
```

Disable the `Table` (in the DB). This process clears the `enabled_at` value in
the `table_metadata` table which will remove the table from the candidate `Table`
selection algorithm used to determine the pool of `Table(s)` available for
synchronization (to add a `Table` back into the pool, simply execute
`enable_table`)
```ruby
database.disable_table(table)
```

Sync the `Table`. This process will pull data from its [remote-]source and
reconcile the new data against the main-table (e.g. perform 'INSERT', 'UPDATE'
and 'DELETE' operations).
```ruby
database.sync_table(table)
```

Sync the `Database`. The result of this process is the same as `sync_table`
except that the process itself will select a candidate table. When multiple
`Table(s)` are configured the least recently pulled and available (`enabled`,
`initialized` and `unlocked`) `Table` will be selected.
```ruby
database.sync
```

Fetch the metadata for a `Table`. This process will return a `Struct`
representation of the current state for the given `Table`
```ruby
database.table_metadata(table)
```

## Contributing

1. Fork it ( http://github.com/jzaleski/purview/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
