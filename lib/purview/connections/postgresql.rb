module Purview
  module Connections
    class PostgreSQL < Base
      private

      def raw_connection_type
        Purview::RawConnections::PostgreSQL
      end
    end
  end
end
