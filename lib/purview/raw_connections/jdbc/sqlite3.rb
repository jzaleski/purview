safe_require('jdbc/sqlite3')

if defined?(Jdbc::SQLite3)
  Jdbc::SQLite3.load_driver

  module Purview
    module RawConnections
      module JDBC
        class SQLite3 < Base
          private

          def url
            "jdbc:sqlite://#{database}"
          end
        end
      end
    end
  end

  Purview::RawConnections::SQLite = Purview::RawConnections::JDBC::SQLite3
end
