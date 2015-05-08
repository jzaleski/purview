if defined?(JRUBY_VERSION) && (require 'jdbc/mysql' rescue nil)
  Jdbc::MySQL.load_driver

  module Purview
    module RawConnections
      module JDBC
        class MySQL < Base
          private

          def engine
            'mysql'
          end
        end
      end
    end
  end

  Purview::RawConnections::MySQL = Purview::RawConnections::JDBC::MySQL
end
