module Purview
  module RawConnections
    module JDBC
      class Base < Purview::RawConnections::Base
        private

        attr_reader :last_sql, :last_statement

        def delete_or_insert_or_update?(sql)
          delete?(sql) || insert?(sql) || update?(sql)
        end

        def engine
          raise %{All "#{Base}(s)" must override the "engine" method}
        end

        def execute_sql(sql, opts={})
          @last_sql = sql
          @last_statement = statement = raw_connection.createStatement
          if select?(sql)
            statement.executeQuery(sql)
          elsif delete_or_insert_or_update?(sql)
            statement.executeUpdate(sql)
            nil
          else
            statement.execute(sql)
            nil
          end
        end

        def extract_rows(result)
          if result
            metadata = result.getMetaData
            column_count = metadata.getColumnCount
            [].tap do |rows|
              while result.next
                rows << {}.tap do |row|
                  (1..column_count).each do |index|
                    column_name = metadata.getColumnName(index)
                    row[column_name] = result.getString(column_name)
                  end
                end
              end
            end
          end
        end

        def extract_rows_affected(result)
          delete_or_insert_or_update?(last_sql) ? last_statement.getUpdateCount : 0
        end

        def new_connection
          java.sql.DriverManager.getConnection(
            url,
            username,
            password
          )
        end

        def url
          "jdbc:#{engine}://#{host}#{port && ":#{port}"}/#{database}"
        end
      end
    end
  end
end
