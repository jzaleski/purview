require 'purview/raw_connections/base'

if defined?(JRUBY_VERSION)
  require 'purview/raw_connections/jdbc/base'

  require 'purview/raw_connections/jdbc/jtds'
  require 'purview/raw_connections/jdbc/mysql'
  require 'purview/raw_connections/jdbc/postgres'
  require 'purview/raw_connections/jdbc/sqlite3'
else
  require 'purview/raw_connections/mysql2'
  require 'purview/raw_connections/pg'
  require 'purview/raw_connections/sqlite3'
  require 'purview/raw_connections/tiny_tds'
end

if \
  !defined?(Purview::RawConnections::MSSQL) &&
  !defined?(Purview::RawConnections::MySQL) &&
  !defined?(Purview::RawConnections::PostgreSQL) &&
  !defined?(Purview::RawConnections::SQLite)
  raise 'Could not initialize raw-connections; please install and require one or more of the following gems: `jdbc-jtds`, `jdbc-mysql`, `jdbc-postgres`, `jdbc-sqlite3`, `mysql2`, `pg`, `sqlite3` and/or `tiny_tds`'
end
