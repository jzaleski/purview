module Purview
  module Connections
    class SQLite < Base
      private

      def raw_connection_type
        Purview::RawConnections::SQLite
      end
    end
  end
end
