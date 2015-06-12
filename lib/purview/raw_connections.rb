require 'purview/raw_connections/base'
require 'purview/raw_connections/jdbc/base'

require 'purview/raw_connections/jdbc/jtds'
require 'purview/raw_connections/jdbc/mysql'
require 'purview/raw_connections/jdbc/postgres'

require 'purview/raw_connections/mysql2'
require 'purview/raw_connections/pg'
require 'purview/raw_connections/tiny_tds'

if \
  !defined?(Purview::RawConnections::MSSQL) &&
  !defined?(Purview::RawConnections::MySQL) &&
  !defined?(Purview::RawConnections::PostgreSQL)
  raise 'Could not initialize raw-connections; please install and require one or more of the following gems: `jdbc-jtds`, `jdbc-mysql`, `jdbc-postgres`, `mysql2`, `pg` and/or `tiny_tds`'
end
