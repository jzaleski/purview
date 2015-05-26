safe_require('jdbc-mysql')

if defined?(Jdbc::MySQL)
  Jdbc::MySQL.load_driver

  module Purview
    module RawConnections
      module JDBC
        class MySQL < Base
          private

          def engine
            'mysql'
          end

          def username
            super || ENV['USER'] || Etc.getlogin
          end
        end
      end
    end
  end

  Purview::RawConnections::MySQL = Purview::RawConnections::JDBC::MySQL
end
