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

Load the `MySQL` client (for `PostgreSQL` simply change 'mysql2' to 'pg')
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
columns are required for all tables)
```ruby
columns = [
  Purview::Columns::Id.new(:id),
  Purview::Columns::String.new(:name, :nullable => false),
  Purview::Columns::String.new(:email, :nullable => false, :limit => 100),
  Purview::Columns::CreatedTimestamp.new(:created_at),
  Purview::Columns::UpdatedTimestamp.new(:updated_at),
]
```

Configure the `Puller` (available puller-types: `MySQL`, `PostgreSQL` & `URI`)
```ruby
puller_opts = {
  :type => Purview::Pullers::URI,
  :uri => 'http://feed.test.com/users',
}
```

Configure the `Parser` (available parser-types: `CSV`, `SQL` & `TSV`)
```ruby
parser_opts = {
  :type => Purview::Parsers::TSV,
}
```

Configure the `Loader` (for `PostgreSQL` simply change `MySQL` to `PostgreSQL`)
```ruby
loader_opts = {
  :type => Purview::Loaders::MySQL,
}
```

Configure the `starting_timestamp` (this is the min-date to pull and can vary
between `Table(s)`)
```ruby
starting_timestamp = Time.parse('2012-01-01 00:00:00Z')
```

Combine all the configuration options and instantiate the `Table`
```ruby
table_opts = {
  :columns => columns,
  :loader => loader_opts,
  :parser => parser_opts,
  :puller => puller_opts,
  :starting_timestamp => starting_timestamp,
}

table = Purview::Tables::Raw.new(
  table_name,
  table_opts
)
```

Set the database-name (this can be anything, but it must exist)
```ruby
database_name = :data_warehouse
```

Combine all the configuration options and instantiate the `Database` (for
`PostgreSQL` simply change `MySQL` to `PostgreSQL`)
```ruby
database_opts = {
  :tables => [table],
}

database = Purview::Databases::MySQL.new(
  database_name,
  database_opts
)
```

Add the `Table` to the `Database` (schema). In order for [the] `Table` to be
`sync[ed]` it *must* be added to [the] `Database`
```ruby
database.add_table(table)
```

Create the `Table` (in the DB). Recommended for testing purposes *only*. For
production environments you will likely want an external process to manage the
schema (for `PostgreSQL` simply change `Mysql2::Error` to `PG::DuplicateTable`)
```ruby
begin
  database.create_table(table)
rescue Mysql2::Error
  # Swallow
end
```

Sync the `Database`. This process will select a [candidate] `Table`, pull data
from its [remote-]source and reconcile the new data against the main-table (e.g.
perform `INSERTs`, `UPDATEs` and `DELETEs`). When multiple `Table(s)` are
configured the least recently pulled and available (`enabled` and not `locked`)
`Table` will be selected (you will likely want to configure some process to load
the schema run the `sync` at regularly scheduled intervals)
```ruby
database.sync
```

## Contributing

1. Fork it ( http://github.com/jzaleski/purview/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
