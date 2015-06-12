safe_require('jdbc/jtds')

if defined?(Jdbc::JTDS)
  Jdbc::JTDS.load_driver

  module Purview
    module RawConnections
      module JDBC
        class JTDS < Base
          private

          def url
            "jdbc:jtds:sqlserver://#{host}#{port && ":#{port}"};databaseName=#{database}"
          end

          def username
            super || ENV['SQLCMDUSER'] || Etc.getlogin
          end
        end
      end
    end
  end

  Purview::RawConnections::MSSQL = Purview::RawConnections::JDBC::JTDS
end
