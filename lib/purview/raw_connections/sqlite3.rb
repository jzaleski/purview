safe_require('sqlite3')

if defined?(SQLite3)
  module Purview
    module RawConnections
      class SQLite3 < Base
        private

        def execute_sql(sql, opts={})
          raw_connection.execute(sql)
        end

        def extract_rows(result)
          result && result.map do |rows|
            rows.reduce({}) do |memo, (key, value)|
              memo[key.to_sym] = value unless key.is_a?(Integer)
              memo
            end
          end
        end

        def extract_rows_affected(result)
          raw_connection.changes
        end

        def new_connection
          ::SQLite3::Database.new(database.to_s, {:results_as_hash => true})
        end
      end
    end
  end

  Purview::RawConnections::SQLite = Purview::RawConnections::SQLite3
end
